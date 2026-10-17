# Despliegue

## Local

```bash
cp env.example .env            # y pon tu RIOT_API_KEY
docker compose up -d db
docker compose run --rm etl run -regions NA1 -tiers MASTER-CHALLENGER
docker compose run --rm etl stats
```

El servicio `etl` comparte red con el contenedor `db`, asi que `MONGO_URI`
apunta a `db:27017` y no a `localhost`.

## Imagen

```bash
docker build -f deploy/Dockerfile -t lol-etl-go:latest --build-arg VERSION=$(git rev-parse --short HEAD 2>/dev/null || echo dev) .
```

El build es `CGO_ENABLED=0` sobre `distroless/static:nonroot`: ~15 MB, sin
shell, usuario no root y raiz de solo lectura. El `runtimePlatform` de la task
definition esta en **ARM64** (Graviton, ~20% mas barato en Fargate); si
construyes en x86 cambialo a `X86_64` o usa `docker buildx --platform linux/arm64`.

## ECR + ECS

```bash
AWS_REGION=us-east-1
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REPO=$ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/lol-etl-go

aws ecr create-repository --repository-name lol-etl-go --region $AWS_REGION
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $REPO
docker tag lol-etl-go:latest $REPO:latest && docker push $REPO:latest

# secretos (la key de produccion NO va en la task definition)
aws secretsmanager create-secret --name lol-etl-go/riot-api-key --secret-string 'RGAPI-...'
aws secretsmanager create-secret --name lol-etl-go/mongo-uri   --secret-string 'mongodb://...'

# sustituye ACCOUNT_ID en el JSON y registra
sed -i'' "s/ACCOUNT_ID/$ACCOUNT_ID/g; s/us-east-1/$AWS_REGION/g" deploy/task-definition.json
aws ecs register-task-definition --cli-input-json file://deploy/task-definition.json

aws ecs run-task \
  --cluster lol-analyze \
  --launch-type FARGATE \
  --task-definition lol-etl-go \
  --network-configuration 'awsvpcConfiguration={subnets=[subnet-xxx],securityGroups=[sg-xxx],assignPublicIp=ENABLED}' \
  --overrides '{"containerOverrides":[{"name":"etl","command":["run","-regions","LA1,NA1","-tiers","DIAMOND-CHALLENGER"]}]}'
```

`assignPublicIp=ENABLED` (o un NAT) es obligatorio: el contenedor necesita
salida a internet para `api.riotgames.com`.

## Permisos que hacen falta

- **executionRole**: `AmazonECSTaskExecutionRolePolicy` + `secretsmanager:GetSecretValue`
  sobre los dos secretos (lo usa el agente para inyectarlos, no el contenedor).
- **taskRole**: solo si accedes a mas servicios AWS. Para Mongo externa no hace falta nada.

## Red hacia Mongo

Fargate no ve el contenedor local de Mongo. Opciones:

| Destino | Nota |
|---|---|
| MongoDB Atlas | lo mas simple; hay que permitir las IPs de salida del NAT o usar peering |
| DocumentDB | misma VPC, requiere `tls=true` y el CA de Amazon en la URI |
| EC2 con mongod | misma VPC y security group que permita 27017 desde el SG de la task |

## Crawl continuo

Un crawl por parche no es un servicio: es un trabajo que acaba. Dos formas:

```bash
# una pasada cuando sale el parche
aws events put-rule --name lol-etl-patch --schedule-expression 'rate(14 days)'

# o cada 6 h para ir recogiendo partidas nuevas del parche en curso
aws events put-rule --name lol-etl-rolling --schedule-expression 'rate(6 hours)'
```

Con `EcsParameters` apuntando a la task definition. El ETL es idempotente: lo
ya descargado no se vuelve a pedir, asi que relanzarlo cada pocas horas solo
trae lo nuevo. El `stopTimeout: 120` da margen a que el SIGTERM cierre los
lotes de escritura antes de que ECS mate el contenedor.
