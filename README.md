# Administrador de Kafka · RutaExpress EV3

Microservicio Spring Boot para consultar, crear, modificar y eliminar tópicos. Envíos publica los eventos operacionales y Reportes los consume para actualizar sus indicadores. Este servicio administra la configuración del clúster y no necesita base de datos.

```text
Administrador → API Gateway (en AWS) → BFF → Kafka Admin → Kafka
Envíos → Kafka → Reportes → PostgreSQL
```

## Contrato HTTP

Todas las rutas se añaden a `/api/report/kafka/topics`. El BFF exige un token de acceso de Cognito con el grupo `Admin`.

| Método y ruta relativa | Resultado |
|---|---|
| `GET` en la ruta base | `200` y la lista de tópicos. |
| `POST` en la ruta base | `201`. Crea un tópico con `name`, `partitions` y `retentionHours`. |
| `GET /{topic}/retention` | `200` y `retentionMillis`. |
| `GET /{topic}/partitions` | `200` y `partitions`. |
| `PUT /{topic}/retention` | `204`. Cambia `retentionHours`. |
| `PUT /{topic}/partitions` | `204`. Aumenta `partitions` al total indicado. |
| `DELETE /{topic}` | `204`. Elimina el tópico de prueba. |

Se permite escribir en tópicos de prueba con el prefijo `rutaexpress.demo.`. El sufijo debe comenzar con una letra minúscula o un dígito y admite hasta 60 caracteres en total, con guion y guion bajo. Las escrituras admiten de 1 a 12 particiones y de 1 a 720 horas de retención. Kafka solo permite aumentar el número de particiones. Las solicitudes inválidas devuelven `400`, un tópico inexistente devuelve `404`, un nombre ya usado devuelve `409` y una operación que no se puede completar por indisponibilidad devuelve `503`.

## Ejecución y verificación

Con Java 21, ejecuta `./mvnw verify` en Linux o `.\mvnw.cmd verify` en Windows. El arranque habitual se hace desde Envíos con `.\iniciar-ev3.ps1`, con los repositorios del proyecto en carpetas hermanas.

| Variable | Valor local por defecto | Uso |
|---|---|---|
| `SERVER_PORT` | `8085` | Puerto HTTP del servicio. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Direcciones iniciales de Kafka. Compose configura los dos brokers. |
| `KAFKA_ADMIN_REPLICAS` | `1` | Réplicas de los tópicos que crea la API. Compose configura 2. |

`GET /actuator/health` comprueba la conexión al clúster. La prueba `.\tests\probar-kafka-ev3.ps1` de Envíos valida el ciclo de vida de un tópico temporal contra los brokers reales y el flujo que alimenta Reportes. El acceso directo a `127.0.0.1:8085` es para pruebas locales. En AWS el servicio queda en la red interna de Docker y el BFF es su único punto de entrada desde API Gateway.

El flujo de colaboración usa las ramas personales para trabajar, `desarrollo` para integrar y `main` para la entrega.
