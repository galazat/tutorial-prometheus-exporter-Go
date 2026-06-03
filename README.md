# Курс: пишем Prometheus exporter с нуля (на примере Kafka)

Привет! Это практический курс, где мы шаг за шагом напишем собственный **Prometheus exporter** для Apache Kafka. Делаем по аналогии с реальным проектом
[danielqsj/kafka_exporter](https://github.com/danielqsj/kafka_exporter/tree/master),
но не копируем его слепо, а разбираем каждое решение: зачем оно нужно и как работает под капотом.

К концу курса у тебя будет:

- **Рабочий exporter** на Go, который собирает метрики Kafka (брокеры, топики, партиции, consumer group lag).
- Понимание модели Prometheus: pull, формат экспозиции, типы метрик, лейблы.
- Готовый артефакт, который запускается **тремя способами**: бинарём на ноде (через systemd), в Docker и в Kubernetes (Deployment + ServiceMonitor + Helm).
- **Универсальный навык**: ты сможешь написать exporter для *любой* системы (БД, очередь, API, железо), а не только для Kafka.

---

## Кому подойдёт

- Ты знаешь основы Go (переменные, структуры, интерфейсы, goroutines — хотя бы на уровне «читаю и понимаю»). Если нет - не страшно, я объясняю каждую конструкцию по ходу.
- Ты слышал про Prometheus, но не писал под него код.
- Ты SRE/DevOps/разработчик, которому нужно мониторить свою систему.

Глубокого знания Kafka не требуется — нужную теорию я даю в уроках 5–7.

---

## Что нужно установить

| Инструмент | Зачем | Проверка |
|------------|-------|----------|
| Go ≥ 1.21 | компилируем exporter | `go version` |
| Docker + docker compose | поднимаем локальную Kafka и собираем образ | `docker version` |
| `curl` | дёргаем `/metrics` руками | `curl --version` |
| (опц.) `kubectl` + `kind`/`minikube` | урок 14 про Kubernetes | `kubectl version` |
| (опц.) `promtool` | валидация формата метрик (урок 11) | `promtool --version` |

> Если чего-то нет - не блокируйся, ставь по ходу. Уроки 1–4 вообще требуют
> только Go.

---

## Как устроен курс

Каждый урок - отдельный `.md` файл. Внутри урока структура одинаковая:

1. **Теория** - что это за концепция и зачем она нужная.
2. **Код шаг за шагом** - маленькие куски с подробным разбором.
3. **Проверка** - как убедиться, что всё работает.
4. **Итог и что дальше**.

Код накапливается от урока к уроку. Финальная собранная версия лежит в репозитории [`kafka-exporter/`]() — туда можно подсмотреть в любой момент, а в конце курса просто запустить.

> **Совет:** не копируй код целиком, печатай руками. Так в голове остаётся в разы больше.

---

## Программа курса

### Часть I. Основы Prometheus (только Go, без Kafka)

1. [Теория: Prometheus и экспортеры](01-prometheus-i-exporters/Part-1-theory-prometheus-i-exporter.md) - pull-модель, формат экспозиции, типы метрик, лейблы, naming convention.
2. [Окружение и первый HTTP-сервер]() - `go mod init`, `net/http`, эндпоинты `/metrics` и `/healthz`.
3. [Первая метрика через client_golang]() - `prometheus`, `promhttp`, дефолтные `go_*`/`process_*` метрики.
4. [Паттерн Custom Collector]() - `Describe`/`Collect`, `NewDesc`, `MustNewConstMetric`. Ключевой паттерн всех exporter'ов.

### Часть II. Собираем метрики Kafka

1. [Подключение к Kafka через Sarama]() - клиент Sarama, локальная Kafka в docker compose, скелет `Exporter`.
2. [Метрики брокеров и топиков]() -  `kafka_brokers`, `kafka_topic_partitions`, current/oldest offset.
3. [Consumer groups и lag]() - самое важное: что такое offset и lag, как их посчитать, метрики групп.

### Часть III. Делаем «как в проде»

1. [Конфигурация: флаги и фильтры]() - CLI через kingpin, regexp-фильтры топиков/групп, кастомные лейблы.
2. [Производительность и конкурентность]() - goroutines, worker pool, кэш метаданных, защита от параллельных scrape.
3.  [Безопасность: TLS и SASL]() - шифрование и аутентификация к Kafka, TLS на самом exporter.
4.  [Тестирование]() - unit/smoke тесты, `promtool`, линтеры.

### Часть IV. Доставка и эксплуатация

1.  [Сборка и Docker]() - version-пакет Makefile, multi-stage Dockerfile.
2.  [Запуск на ноде (systemd)]() - unit-файл, отдельный пользователь, `scrape_config` в Prometheus.
3.  [Запуск в Kubernetes]() - Deployment, Service, ServiceMonitor (Prometheus Operator), Helm chart, Grafana.

### Часть V. Обобщение

1.  [Как написать exporter для любой системы]() - чеклист, универсальный шаблон, best practices и антипаттерны.

---

## Метрики, которые мы реализуем

К концу части II наш exporter будет отдавать:

| Метрика | Тип | Лейблы | Смысл |
|---------|-----|--------|-------|
| `kafka_brokers` | gauge | — | число брокеров в кластере |
| `kafka_topic_partitions` | gauge | `topic` | число партиций топика |
| `kafka_topic_partition_current_offset` | gauge | `topic`,`partition` | последний offset (конец лога) |
| `kafka_topic_partition_oldest_offset` | gauge | `topic`,`partition` | самый старый offset (начало лога) |
| `kafka_consumergroup_current_offset` | gauge | `consumergroup`,`topic`,`partition` | закоммиченный offset группы |
| `kafka_consumergroup_lag` | gauge | `consumergroup`,`topic`,`partition` | отставание группы (lag) |
| `kafka_consumergroup_members` | gauge | `consumergroup` | число участников группы |

Это «ядро» настоящего kafka_exporter. Дальше (TLS, фильтры, репликация) - это обвязка вокруг этого ядра.

---

Пора переходить к изучению! Открывай [урок 1]()
