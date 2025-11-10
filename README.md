![Python](https://img.shields.io/badge/python-3.11-3776AB?logo=python)
![Elasticsearch](https://img.shields.io/badge/Elasticsearch-8.x-005571?logo=elasticsearch)
![Kibana](https://img.shields.io/badge/Kibana-8.x-005571?logo=kibana)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![License](https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg)

# Scraper de Partidas (CBLOL e Multi-Região)

> Serviço Python desacoplado para coleta e indexação de partidas profissionais em Elasticsearch, 
> Utilizando LoLEsports (Persisted Gateway) e Riot Match-V5.

## Tabela de Conteúdos

- [Quick Start](#quick-start)
- [Stack Tecnológico](#stack-tecnológico)
- [Arquitetura](#arquitetura)
- [Setup](#setup)
- [Uso](#uso)
- [Estrutura](#estrutura)
- [Variáveis de Ambiente](#variáveis-de-ambiente)
- [Troubleshooting](#troubleshooting)
- [Licença](#licença)

<details>
<summary> Quick Start (clique para expandir) </summary>

### Opção 1: Com Docker (Recomendado)

```bash
# Subir Elasticsearch e Kibana do stack do scraper
docker compose -f League-Data-Scraping-And-Analytics-master/ProStaff-Scraper/docker-compose.yml up -d elasticsearch kibana

# Executar pipeline CBLOL (usa RIOT_API_KEY e ESPORTS_API_KEY do .env do diretório)
docker compose -f League-Data-Scraping-And-Analytics-master/ProStaff-Scraper/docker-compose.yml run --rm scraper \
  python pipelines/cblol.py --league CBLOL --limit 50

# Acessar Kibana
start http://localhost:5601  # Windows
```

### Opção 2: Sem Docker (Local)

```bash
# Criar virtualenv e instalar dependências (rodando a partir da raiz do repo)
python -m venv .venv
.venv/Scripts/activate  # Windows
pip install -r League-Data-Scraping-And-Analytics-master/ProStaff-Scraper/requirements.txt

# Exportar variáveis (ou usar .env do diretório com python-dotenv)
# Execute o pipeline
python League-Data-Scraping-And-Analytics-master/ProStaff-Scraper/pipelines/cblol.py --league CBLOL --limit 20
```

**Elasticsearch**: `http://localhost:9200`
**Kibana**: `http://localhost:5601`

</details>

## Stack Tecnológico

- **Linguagem**: Python 3.11
- **HTTP Client**: httpx com `tenacity` (backoff/retry)
- **Modelagem**: pydantic
- **Serialização**: orjson
- **Configuração**: python-dotenv
- **Busca/Analytics**: Elasticsearch 8.x + Kibana 8.x

## Arquitetura

Para o diagrama completo e detalhes dos componentes, consulte:

- `docs/Arquitetura.md`

## Setup

1. Copie `.env.example` para `.env` dentro de `League-Data-Scraping-And-Analytics-master/ProStaff-Scraper/` e preencha chaves.
2. Suba Elasticsearch/Kibana com `docker compose -f League-Data-Scraping-And-Analytics-master/ProStaff-Scraper/docker-compose.yml up -d elasticsearch kibana`.
3. (Opcional) Crie índices manualmente no Kibana Dev Tools ou deixe o scraper criar via `ensure_index`.

## Uso

Execute o pipeline CBLOL (POC) para ingestão:

```bash
# Com Docker
docker compose -f League-Data-Scraping-And-Analytics-master/ProStaff-Scraper/docker-compose.yml run --rm scraper \
  python pipelines/cblol.py --league CBLOL --limit 50

# Sem Docker
python League-Data-Scraping-And-Analytics-master/ProStaff-Scraper/pipelines/cblol.py --league CBLOL --limit 20
```

O pipeline:
- Descobre eventos/schedules via LoLEsports
- Compõe `match_id` com base no `gameId` e região (ex.: `BR1_<gameId>`)
- Busca detalhes e timeline no Match-V5 (região mapeada)
- Normaliza e indexa em `lol_pro_matches` e `lol_timelines`

## Estrutura

- `providers/esports.py`: chamadas ao LoLEsports Persisted Gateway
- `providers/riot.py`: chamadas ao Riot Match/Account V5
- `indexers/elasticsearch_client.py`: cliente e helpers de bulk
- `indexers/mappings.py`: mappings dos índices
- `pipelines/cblol.py`: orquestração da ingestão CBLOL

## Variáveis de Ambiente

Defina em `League-Data-Scraping-And-Analytics-master/ProStaff-Scraper/.env` (veja `.env.example`):

- `RIOT_API_KEY`: chave da Riot para Match-V5
- `ESPORTS_API_KEY`: chave da LoLEsports Persisted Gateway (se aplicável)
- `ELASTICSEARCH_URL`: URL do Elasticsearch (ex.: `http://localhost:9200`)
- `KIBANA_URL`: URL do Kibana (ex.: `http://localhost:5601`)
- `PLATFORM_REGION_DEFAULT`: região padrão (ex.: `americas`)
- `PLATFORM_REGION_ALLOWED`: lista permitida (ex.: `americas,europe,asia`)

## Troubleshooting

- Falha ao conectar no Elasticsearch: verifique `ELASTICSEARCH_URL` e se o serviço está up.
- Rate limit na Riot API: o scraper usa backoff exponencial; tente reduzir `--limit`.
- Índices inexistentes: o scraper tenta criar via `ensure_index`; ou crie no Kibana Dev Tools.

## Atualização de Campeões

- O scraper usa `championName` do Match-V5, então novos campeões são ingeridos automaticamente.
- Para scripts auxiliares que dependem de mapeamento `championId -> name`, há suporte a atualização automática via Data Dragon:
  - Arquivo: `providers/ddragon.py`
  - Na primeira execução, gera/atualiza `champions.json` com a versão mais recente.
  - Você pode chamar `load_champion_map(Path(__file__).resolve().parent)` para obter o mapeamento.

## Licença

CC BY-NC-SA 4.0