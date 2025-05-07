## Lab 6

Данная программа определяет среднюю цену на однокомнатные квартиры в различных
регионах

Данные:

- input_data.csv - [источник набора данных][kaggle]
- [regions.txt][regions]

## Необходимые инструменты

Для запуска [приложения][app] необходимы следующие инструменты:

- kafka
  - в качестве альтернативы, можно _kafka_ развернуть в _docker контейнере_ при
    помощи команды `docker-compose up` ([docker-compose файл][docker-compose])
- python (версия >= 3.13)

### Python зависимости

Все необходимые зависимости представлены в файле [pyproject][pyproject]

Зависимости:

- kafka-python (версия >=2.2.4)

Все необходимые _python зависимости_ можно установить при помощи следующих
команд:

```bash
# pip
pip install kafka-python

# uv
uv sync
```

## Запуск программы

Для работы программы необходимо, чтобы была запущена _kafka_

**отправка данных:**

```bash
python main.py --producer

# uv
uv run main.py --producer
```

**преобразование данных:**

```bash
python main.py --transformer

# uv
uv run main.py --transformer
```

**фильтрация данных:**

```bash
python main.py --filter

# uv
uv run main.py --filter
```

**агрегация данных:**

```bash
python main.py --aggregator

# uv
uv run main.py --aggregator
```

[kaggle]:
  https://www.kaggle.com/datasets/mrdaniilak/russia-real-estate-2021/data
[regions]: ../data/regions.txt
[app]: main.py
[docker-compose]: docker-compose.yml
[pyproject]: pyproject.toml
