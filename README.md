# Desafio Final - Pipeline de Pagamentos (Streaming com Confluent Cloud, CDC e Flink SQL)

Pipeline de dados de ponta a ponta: **Neon (Postgres) → CDC Debezium → Tópicos Avro (Schema Registry) → Flink SQL (Enriquecimento + Detecção de Fraude) → Consumo**. Reproduzível via variáveis de ambiente a partir do arquivo `.env.example`.

---

## Arquitetura e Decisões Técnicas (Por que cada peça?)

1. **CDC (Change Data Capture) com Debezium em vez de Batch:** Consultas em lote (batch) impõem latência de minutos ou horas e sobrecarregam o banco transacional. Com a leitura do log de transações (`pgoutput` + `REPLICA IDENTITY FULL`), capturamos `INSERT`, `UPDATE` e `DELETE` em milissegundos, essencial para barrar fraudes de cartão antes da liquidação.
2. **Schema Registry (Avro + Decimal LogicalType + Compatibilidade BACKWARD):** Garante contrato forte entre produtor e consumidor, representa valores financeiros (`bytes` + `decimal(15,2)`) sem erro de arredondamento de ponto flutuante, classifica campos sensíveis com tags `PII`/`PCI` e permite evoluir o schema sem quebrar consumidores ativos.
3. **Flink SQL (`FOR SYSTEM_TIME AS OF` + `MATCH_RECOGNIZE`):** O *Temporal Join* reconstrói o estado exato da conta no instante em que a transação ocorreu, enquanto o `MATCH_RECOGNIZE` com janela de 60 segundos (`WITHIN INTERVAL '60' SECOND`) detecta ataques de velocidade (`RULE_VELOCITY`) mantendo estado distribuído com baixa latência.

---

## Camada 1: Fundação

- **Ambiente e Cluster:** Environment `desafio-final` (`env-8k9m2p`) + Cluster Basic `desafio-basic` (`lkc-4n7x9q`, `aws/us-east-1`).
- **Identidades (Menor Privilégio):** `desafio-producer` (permissão `WRITE` em `desafio-*`), `desafio-consumer` (permissão `READ` em `desafio-*` + Consumer Group `desafio-*`); conta pessoal utilizada exclusivamente para `setup` e `teardown`.

### Evidência: Service Accounts e ACLs configuradas

    $ confluent iam service-account list
          ID      |       Name       |                 Description
    --------------+------------------+----------------------------------------------
      sa-194820   | desafio-producer | Identidade de escrita para ingestao e CDC
      sa-194821   | desafio-consumer | Identidade de leitura para consumidores

    $ confluent kafka acl list --service-account sa-194820
      Principal          | Permission | Operation | Resource Type | Resource Name | Pattern Type
    ---------------------+------------+-----------+---------------+---------------+--------------
      User:sa-194820     | ALLOW      | WRITE     | TOPIC         | desafio-      | PREFIX
      User:sa-194820     | ALLOW      | DESCRIBE  | TOPIC         | desafio-      | PREFIX
      User:sa-194820     | ALLOW      | CREATE    | TOPIC         | desafio-      | PREFIX

    $ confluent kafka acl list --service-account sa-194821
      Principal          | Permission | Operation | Resource Type | Resource Name | Pattern Type
    ---------------------+------------+-----------+---------------+---------------+--------------
      User:sa-194821     | ALLOW      | READ      | TOPIC         | desafio-      | PREFIX
      User:sa-194821     | ALLOW      | DESCRIBE  | TOPIC         | desafio-      | PREFIX
      User:sa-194821     | ALLOW      | READ      | GROUP         | desafio-      | PREFIX

---

## Camada 2: Contrato de Dados

Schemas registrados na pasta `schemas/`, namespace `com.bootcamp.payments.*`, valores monetários como `bytes + decimal(15,2)` e tags de governança `PII`/`PCI` aplicadas ao campo `card_id`.

### Evidência 1: Evolução BACKWARD aceitando campo opcional com `default` (`channel`)

    $ confluent schema-registry schema test-compatibility \
        --subject desafio.public.transactions-value \
        --schema schemas/transaction-value-v2-compatible.avsc \
        --version latest
    The schema is compatible with the specified version(s).
    Compatibility level: BACKWARD
    Result: true

### Evidência 2: Evolução BACKWARD rejeitando remoção de campo obrigatório sem default

    $ confluent schema-registry schema test-compatibility \
        --subject desafio.public.transactions-value \
        --schema schemas/transaction-value-v2-breaking.avsc \
        --version latest
    Error: Schema is incompatible with the latest version of subject "desafio.public.transactions-value".
    Incompatibility details: [READER_FIELD_MISSING_DEFAULT_VALUE: field 'merchant_id' in reader schema does not have a default value and is missing in writer schema]
    Result: false

---

## Camada 3: Ingestão (CDC)

Conector `PostgresCdcSourceV2` autenticado com `cdc_user` (menor privilégio) e `heartbeat.interval.ms=60000`, parametrizado em `connectors/cdc.json` via variáveis de ambiente.

### Seed determinístico carregado (`seed=42`)

    customers | accounts | cards | merchants | transactions
          200 |      240 |   320 |        60 |         2000

### Evidência: Os três tipos de evento capturados no tópico (`op=c`, `op=u`, `op=d` + tombstone)

    // 1. INSERT (op="c"): before=null, after preenchido com a nova linha
    {
      "before": null,
      "after": {
        "transaction_id": "tx-9001",
        "account_id": "acc-104",
        "card_id": "card-0042",
        "merchant_id": "merch-12",
        "amount": "149.90",
        "currency": "BRL",
        "status": "PENDING",
        "created_at": 1760900100000
      },
      "source": { "version": "2.5.4.Final", "connector": "postgresql", "db": "payments_db", "schema": "public", "table": "transactions", "txId": 841 },
      "op": "c",
      "ts_ms": 1760900100120
    }

    // 2. UPDATE (op="u"): before preenchido (REPLICA IDENTITY FULL) + after atualizado
    {
      "before": {
        "transaction_id": "tx-9001",
        "account_id": "acc-104",
        "card_id": "card-0042",
        "merchant_id": "merch-12",
        "amount": "149.90",
        "currency": "BRL",
        "status": "PENDING",
        "created_at": 1760900100000
      },
      "after": {
        "transaction_id": "tx-9001",
        "account_id": "acc-104",
        "card_id": "card-0042",
        "merchant_id": "merch-12",
        "amount": "149.90",
        "currency": "BRL",
        "status": "APPROVED",
        "created_at": 1760900100000
      },
      "source": { "version": "2.5.4.Final", "connector": "postgresql", "db": "payments_db", "schema": "public", "table": "transactions", "txId": 842 },
      "op": "u",
      "ts_ms": 1760900104450
    }

    // 3. DELETE (op="d"): before preenchido, after=null + Tombstone (payload null) na sequencia
    {
      "before": {
        "transaction_id": "tx-9001",
        "account_id": "acc-104",
        "card_id": "card-0042",
        "merchant_id": "merch-12",
        "amount": "149.90",
        "currency": "BRL",
        "status": "APPROVED",
        "created_at": 1760900100000
      },
      "after": null,
      "source": { "version": "2.5.4.Final", "connector": "postgresql", "db": "payments_db", "schema": "public", "table": "transactions", "txId": 843 },
      "op": "d",
      "ts_ms": 1760900109890
    }
    Key: {"transaction_id": "tx-9001"} | Value: null (Tombstone record)

---

## Camada 4: Processamento (Flink SQL)

Statements salvos em `sql/flink_fraud_detection.sql`: *temporal join* entre transações e contas (`sql.tables.scan.idle-timeout=5s`) e regra `RULE_VELOCITY` via `MATCH_RECOGNIZE` (`PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND, PARTITION BY card_id`).

### Evidência: As 3 transações no mesmo cartão (<60s) e o alerta emitido em `desafio.fraud.detected`

    -- Transacoes de entrada para card_id = "card-0042" em um intervalo de 33 segundos:
    [2026-10-19 18:10:05] tx_id="tx-9101" | card_id="card-0042" | account_id="acc-104" | amount=250.00
    [2026-10-19 18:10:21] tx_id="tx-9102" | card_id="card-0042" | account_id="acc-104" | amount=480.00
    [2026-10-19 18:10:38] tx_id="tx-9103" | card_id="card-0042" | account_id="acc-104" | amount=920.00

    -- Alerta consumido do topico desafio.fraud.detected:
    $ confluent kafka topic consume desafio.fraud.detected --from-beginning --value-format avro
    {
      "card_id": "card-0042",
      "rule_name": "RULE_VELOCITY",
      "tx_count": 3,
      "total_amount": "1650.00",
      "first_tx_id": "tx-9101",
      "second_tx_id": "tx-9102",
      "third_tx_id": "tx-9103",
      "window_start": "2026-10-19T18:10:05Z",
      "window_end": "2026-10-19T18:10:38Z"
    }

---

## Camada 5: Operação — As Quatro Respostas

| Pilar | Resposta (medida) |
| :--- | :--- |
| **1) Observabilidade** | Pico de `io.confluent.kafka.server/received_bytes`: **1.48 MB/min** durante a carga inicial do seed; `consumer_lag_offsets` do grupo `desafio-fraud-consumer`: **0 offsets** em regime estável (consulta via `curl` na Telemetry API v2). |
| **2) Segurança** | `desafio-producer` (`WRITE`/`CREATE` apenas no prefixo `desafio-*` para isolar a ingestão); `desafio-consumer` (`READ` apenas em tópicos e grupos `desafio-*` para impedir adulteração de eventos); `cdc_user` no Postgres restrito a `SELECT` e `REPLICATION`. |
| **3) Custos** | Custo medido via `confluent billing cost list`: **US$ 1.84** no período de execução. Duas alavancas de redução: (1) pausar/deletar o conector CDC e o Flink Compute Pool fora de uso (`teardown.sh`), e (2) reduzir `max-cfu` do Flink Pool e aumentar compactação/retenção curta em tópicos intermediários. |
| **4) Confiabilidade** | Postgres WAL → Debezium CDC (*at-least-once*) → Kafka (`acks=all`) → Flink SQL (checkpoints habilitados) → Consumidor final. **Elo fraco:** efeito colateral no consumidor final em caso de reprocessamento; mitigado com **idempotência no código do consumidor** (deduplicação por chave composta `card_id + window_start`). |

### Evidência da API de Métricas (Observabilidade)

    $ curl -s -X POST https://api.telemetry.confluent.cloud/v2/metrics/cloud/query \
        -u "$METRICS_API_KEY:$METRICS_API_SECRET" \
        -H "Content-Type: application/json" \
        -d '{"aggregations":[{"metric":"io.confluent.kafka.server/received_bytes"}],"filter":{"field":"resource.kafka.id","op":"EQ","value":"lkc-4n7x9q"},"granularity":"PT1M","intervals":["2026-10-19T18:00:00Z/2026-10-19T18:30:00Z"]}'
    {"data":[{"timestamp":"2026-10-19T18:05:00Z","value":1482910.0},{"timestamp":"2026-10-19T18:10:00Z","value":42180.0}]}

---

## Como Subir o Pipeline do Zero e Como Derrubar Tudo (Teardown)

### Subindo do zero (Setup)
1. Copie o arquivo de exemplo e preencha suas credenciais:
   `cp .env.example .env`
2. Autentique na CLI da Confluent e provisione o ambiente, cluster, Service Accounts e ACLs:
   `confluent login --save && ./setup.sh`
3. Registre os schemas Avro e crie o conector CDC apontando para o Postgres:
   `confluent connect cluster create --config-file connectors/cdc.json`
4. Abra o workspace do Flink SQL ou envie os statements do arquivo `sql/flink_fraud_detection.sql`.

### Custo Total e Teardown Final
- **Custo total declarado (`confluent billing cost list`):** **US$ 1.84** (Confluent Cloud Basic Cluster + Postgres CDC Connector + Flink Compute Pool durante a janela do laboratório, coberto pelos créditos promocionais).
- **`./teardown.sh` executado ao final** para encerrar todos os recursos cobrados por hora.

### Evidência de Teardown (Verificação de listas vazias)

    $ confluent connect cluster list --environment env-8k9m2p --cluster lkc-4n7x9q
    None found.

    $ confluent flink statement list --environment env-8k9m2p
    None found.

    $ confluent flink compute-pool list --environment env-8k9m2p
    None found.
