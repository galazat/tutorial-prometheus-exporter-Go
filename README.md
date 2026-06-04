# Цикл статей: пишем Prometheus exporter на Go 

Привет! Это практический курс в формате цикла статей, где мы шаг за шагом напишем собственный Prometheus exporter на Go с нуля. В качестве примера реализации возьмем kafka_exporter, поймем зачем оно нужно и как работает под капотом.

Цикл статей рассчитан на 15 уроков, где мы шаг за шагом будем с нуля забрираться и писать свой собственный kafka_exporter. Каждый последующий урок будет выходить постепенно, друг за другом, исходный код exporter будем коммитить в репозиторий [kafka_exporter_prometheus](https://github.com/galazat/kafka_exporter_prometheus) 

К концу курса у тебя будет:

- Рабочий exporter на Go, который собирает метрики Kafka (брокеры, топики, партиции, consumer group lag).
- Понимание модели Prometheus: pull, формат экспозиции, типы метрик, лейблы.
- Готовый артефакт, который запускается тремя способами: бинарём на ноде (через systemd), в Docker и в Kubernetes (Deployment + ServiceMonitor + Helm).
- Навыки: ты сможешь написать exporter для любой системы (БД, очередь, API, железо), а не только для Kafka.

---

## Кому подойдёт

- Ты знаешь основы Go. Если нет - не страшно, я постараюсь максимально подробно разбирать каждую конструкцию.
- Ты слышал про Prometheus, но не писал под него код.
- Ты SRE/DevOps/разработчик, которому нужно мониторить свою систему.

---

## Что нам понадобится

| Инструмент | Зачем | Проверка |
|------------|-------|----------|
| Go ≥ 1.21 | компилируем exporter | `go version` |
| Docker + docker compose | поднимаем локальную Kafka и собираем образ | `docker version` |
| `curl` | дёргаем `/metrics` руками | `curl --version` |
| (опционально) `kubectl` + `minikube` | деплой в Kubernetes | `kubectl version` |
| (опционально) `promtool` | валидация формата метрик | `promtool --version` |

---

## Как устроен курс

Каждый урок - отдельный `.md` файл. Внутри урока дается небольшая теория для лучшего погружения в материал, куски кода с объяснением и разбором и проверка написанного нами кода.

Код накапливается от урока к уроку. Финальная собранная версия лежит в репозитории [kafka_exporter_prometheus](https://github.com/galazat/kafka_exporter_prometheus)  — туда можно подсмотреть в любой момент, а в конце курса просто запустить.

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

