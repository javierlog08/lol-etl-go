# lol-etl-go

ETL concurrente en Go para la **API pública de Riot (League of Legends)**: censa
jugadores por región y rango de elo, descubre sus partidas y las carga en MongoDB
guardando el documento de `match-v5` tal como lo devuelve el API.

Pensado para construir **poblaciones de referencia**: "todos los junglas Master a
Challenger de NA en el parche actual", para luego comparar a un jugador contra
jugadores del mismo campeón, el mismo elo y el mismo parche.

```
census  →  padrón de jugadores por tier (league-v4)
discover →  ids de partida de cada jugador (match-v5)
fetch    →  detalle de cada partida, y timeline si se pide
```

Las etapas 2 y 3 van **solapadas**: los ids fluyen por un canal a los descargadores,
sin barrera entre fases. El único que marca el ritmo es el limitador de rate.

## Arranque rápido

```bash
cp env.example .env          # y pon tu RIOT_API_KEY
docker compose up -d db

docker compose run --rm etl census -regions NA1 -tiers MASTER-CHALLENGER
docker compose run --rm etl crawl  -regions NA1 -tiers MASTER-CHALLENGER
docker compose run --rm etl stats
```

Sin Docker:

```bash
export RIOT_API_KEY='RGAPI-...'
export MONGO_URI='mongodb://localhost:27017'
go run ./cmd/etl run -regions NA1 -tiers MASTER-CHALLENGER
```

`crawl` tira del padrón que deja `census`, así que la primera vez usa `run` (hace las
dos) o censa antes. Después, `crawl` suelto recoge lo nuevo del parche en curso.

**Mide antes de comprometer disco**: `-dry-run` cuenta ids únicos sin descargar las
partidas, y `-discover-limit N` muestrea N jugadores por región.

```bash
docker compose run --rm etl crawl -regions NA1 -tiers MASTER-CHALLENGER \
    -discover-limit 100 -dry-run
```

## Flags

| Flag | Default | Qué hace |
|---|---|---|
| `-regions` | `LA1` | plataformas separadas por coma, o `all` (las 14 vivas) |
| `-tiers` | `DIAMOND-CHALLENGER` | rango (`GOLD-MASTER`) o lista (`GOLD,PLATINUM`) |
| `-queue` | `420` | cola de match-v5; `420` = ranked solo/duo, `0` = todas |
| `-queue-name` | `RANKED_SOLO_5x5` | cola de league-v4 |
| `-patch` | el actual | parche a conservar; `any` = no filtrar |
| `-since` | `336h` | ventana de historial que se pide (14 días ≈ un parche) |
| `-timelines` | `false` | baja también timelines: 7x disco, 2x llamadas |
| `-discover-workers` | `16` | goroutines pidiendo ids |
| `-fetch-workers` | `24` | goroutines bajando partidas |
| `-max-players` | sin tope | corta el censo por región |
| `-discover-limit` | sin tope | jugadores a consultar por región (muestreo) |
| `-dry-run` | `false` | no escribe; cuenta volumen |
| `-log-level` | `info` | `debug` / `info` / `warn` / `error` |

Todos tienen equivalente en variable de entorno (`ETL_REGIONS`, `ETL_TIERS`, ...), para
usarlo en una task definition o un cron. Ver `env.example`.

## Qué deja en Mongo

| Colección | Contenido | Clave |
|---|---|---|
| `players` | padrón: `tier`, `rank`, `league_points`, `region`, `tier_snapshot_at`, `key_fp` | `puuid` |
| `matches` | JSON de `match-v5` crudo + `_etl` (huella de key y fecha) | `metadata.matchId` |
| `timelines` | JSON de timeline crudo | `metadata.matchId` |
| `etl_progress` | avance por tarea | `task_name` |

El tier se guarda **etiquetado en el momento de la captura**, porque la gente sube y
baja de rango dentro de un mismo parche.

## El limitador de rate

Riot limita **por host y por método**, con ventanas que vienen en las cabeceras con la
forma `"500:10,30000:600"` (peticiones:segundos). Aquí es un bucket por clave con un
anillo de timestamps, que consultan todas las goroutines antes de salir.

No hay ningún límite cableado: arranca conservador y **se reconfigura con la cabecera
`X-App-Rate-Limit` de la primera respuesta**, así que el mismo binario aprovecha una
production key entera sin tocar configuración, y respeta `Retry-After` y
`X-Rate-Limit-Type` cuando llega un 429.

Los límites de app aplican **por host**, y match-v5 usa rutas regionales
(`americas`, `europe`, `asia`, `sea`). Las cuatro corren en paralelo con cupo propio,
así que el tiempo total lo marca la ruta más cargada, no la suma.

| Tipo de key | Límite de app | Caducidad |
|---|---|---|
| Development | `20:1, 100:120` | 24 h |
| Personal | `20:1, 100:120` | no caduca |
| Production | `500:10, 30000:600` | no caduca |

Entre personal y production hay un factor **60** en caudal. Compruébalo con
`curl -D -` sobre cualquier endpoint y mira `x-app-rate-limit`.

## Lo que ahorra llamadas

- Cada partida se descubre hasta 10 veces, una por participante. Los ids se deduplican
  en memoria y se contrastan **en lotes de 1.000** contra Mongo antes de pedir nada.
- `league-v4` ya devuelve `puuid`: no hace falta `summoner-v4`, una llamada menos por
  jugador.
- `startTime` recorta el historial en el servidor en vez de bajar y descartar.
- `discovered_patch` por jugador hace el crawl **reanudable**: matarlo y relanzarlo no
  repite ni una llamada. `stopTimeout` en ECS da margen a que SIGTERM cierre los lotes.

## Rarezas del API de Riot que cuesta descubrir

Medido en octubre de 2026, parche 16.20:

1. **Los `puuid` van cifrados por API key.** La misma cuenta devuelve un puuid distinto
   con cada key, y eso incluye los `info.participants[].puuid` dentro de cada partida.
   Usar un puuid de otra key da `400 Bad Request - Exception decrypting`. Data crawleada
   con dos keys distintas **no se puede cruzar por puuid**. Por eso cada documento lleva
   `key_fp` (huella SHA de la key) y el crawl solo consulta jugadores de su propia
   huella, purgando el padrón de otras al arrancar.
2. **`MASTER` no se puede paginar.** `league-v4/entries/.../MASTER/I` responde
   `400 — must be one of [DIAMOND,EMERALD,PLATINUM,GOLD,SILVER,BRONZE,IRON]`. Los tiers
   apex solo existen por `masterleagues` / `grandmasterleagues` / `challengerleagues`,
   que devuelven la liga completa de una vez. En las regiones grandes Master sale con
   exactamente 10.000 entradas.
3. **`PH2` y `TH2` no resuelven en DNS.** Riot sirve Filipinas y Tailandia desde `SG2`.
   Quedan 14 plataformas vivas.
4. **Hace falta `User-Agent` de navegador.** Sin él, Cloudflare responde
   `403 error code: 1010` aunque la key sea válida, y el diagnóstico engaña.
5. **No pongas `Accept-Encoding: gzip` a mano** en Go: `net/http` solo descomprime de
   forma transparente cuando la cabecera la añade él. Si la pones tú, el cuerpo llega
   comprimido y el JSON falla con `invalid character '\x1f'`.
6. **Una key recién generada devuelve `401 Unknown apikey` durante 1-2 minutos**, hasta
   que propaga. No es que esté mal: hay que reintentar.
7. **El historial llega hasta ~2 años atrás.** `matches/by-puuid/.../ids` no devuelve
   nada anterior, por mucho que acotes `startTime`.

## Volumen, con números medidos

82 KB por partida, 575 KB por timeline, comprime al 14% con gzip.

Censo de octubre 2026, Master a Challenger: **101.570 cuentas** en 14 regiones. En NA,
11.000 cuentas (10.000 Master + 700 GM + 300 Challenger) rinden **51 partidas por
jugador** en 14 días, y cada partida la descubren 9,8 jugadores del censo, o sea
**~57.300 partidas únicas** del parche. Sin timelines son ~69.800 llamadas: 23 h con
una key personal, 23 min con una production.

Para una referencia estadística no hace falta la población entera: con `-discover-limit`
sobre unos 1.000 jugadores salen ~18.000 partidas (unas 5-7 h), que dan ~600 junglas por
campeón. El divisor de deduplicación baja al muestrear (en alto elo los jugadores se
cruzan constantemente), así que el volumen **no** cae en proporción.

## Despliegue

`deploy/` trae el `Dockerfile` (multi-stage a `distroless/static`, imagen de 11 MB,
usuario no root, raíz de solo lectura) y una **task definition de ECS Fargate** con la
key inyectada desde Secrets Manager y logs en CloudWatch. Instrucciones en
[`deploy/README.md`](deploy/README.md).

## Datos y términos de uso

Este repositorio es **solo código**: no incluye ni publica data de partidas ni de
jugadores. Para usarlo necesitas tu propia clave de
[developer.riotgames.com](https://developer.riotgames.com) y quedas sujeto a los
términos del API de Riot, incluidos sus límites de rate — que el limitador respeta, pero
cuyo cumplimiento es responsabilidad de quien lo ejecuta.

La clave se lee de `RIOT_API_KEY` o de `RIOT_API_KEY_FILE` y **nunca** se escribe en
disco ni se registra en el log; de la clave solo se persiste un hash truncado de 12
caracteres (`key_fp`) para detectar data de keys distintas.

Si cargas data de otros jugadores, recuerda que los `puuid` identifican cuentas reales:
no los publiques.

No está afiliado ni respaldado por Riot Games. League of Legends es una marca registrada
de Riot Games, Inc.

## Licencia

[MIT](LICENSE)
