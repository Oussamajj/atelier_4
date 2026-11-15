# Analytics temps réel — Spring Cloud Stream + Kafka + Kafka Streams

Projet réalisé en suivant la vidéo **« Event Driven Architecture - Spring Cloud Streams - KAFKA - KAFKA Streams »**
du Professeur Mohamed YOUSSFI, adapté et signé **Oussama** (groupe `ma.oussama`, artefact `oussama-kafka-analytics`).

## Principe

Un **Supplier** simule un capteur de visites de pages (P1 / P2) toutes les 200 ms dans le topic `T3`.
**Kafka Streams** lit `T3`, compte les visites par page sur une fenêtre glissante de **5 secondes**,
stocke le résultat dans le store matérialisé `count-store` et publie dans `T4`.
Un contrôleur REST expose les résultats via **Server-Sent Events** (`/analytics`) vers une page HTML
qui affiche deux courbes en temps réel (SmoothieCharts).

Un **Consumer** Spring Cloud Stream lit également les événements du topic `T2`, et l'endpoint
`GET /publish?name=P1&topic=T2` permet de publier manuellement un `PageEvent`.

## Démarrer

1. Lancer Kafka + Zookeeper :

```bash
docker compose up -d
```

2. Lancer l'application :

```bash
mvn spring-boot:run
```

3. Ouvrir le dashboard : http://localhost:8080/

> Au démarrage, Kafka Streams met environ 30 secondes avant de produire les premiers calculs
> (fenêtre de comptage). Les courbes apparaissent ensuite progressivement.

## Endpoints utiles

| Endpoint | Description |
| --- | --- |
| `GET /` | Dashboard temps réel (SSE + SmoothieCharts) |
| `GET /analytics` | Flux SSE des comptages (P1, P2) chaque seconde |
| `GET /publish?name=P1&topic=T2` | Publie un `PageEvent` aléatoire dans le topic demandé |

## Topics

| Topic | Rôle |
| --- | --- |
| `T2` | Événements publiés via `/publish` et lus par le Consumer |
| `T3` | Événements générés par le Supplier (source du stream) |
| `T4` | Résultats du traitement Kafka Streams |

## Console Kafka (optionnel)

```bash
docker exec -it bdcc-kafka-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic T3 --from-beginning
```

## Structure

```?/
src/main/java/ma/oussama/kafka/
├── OussamaKafkaApplication.java
├── events/PageEvent.java                    # record : name, user, date, duration
├── handlers/PageEventHandler.java           # Consumer, Supplier, Function Kafka Streams
└── controllers/PageEventController.java     # /publish et /analytics (SSE)
src/main/resources/
├── application.properties                   # bindings Spring Cloud Stream
├── banner.txt                               # bannière "Oussama"
└── static/index.html                        # dashboard temps réel
docker-compose.yml                           # Zookeeper + Kafka
```
