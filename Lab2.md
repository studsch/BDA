## MapReduce

### "Очень большой файл целых чисел"
Для того, чтобы создать "очень большой файл целых чисел" был написан [скрипт](cmd/generate/main.go). Чтобы его запустить необходимо запустить следующую команду:

```bash
go run cmd/generate/main.go
```

После запуска скрипта будет создан ["очень большой файл целых чисел"](very_large_integer_file.txt)

### Реализация
Спроектирован алгоритм MapReduce, который принимают на входе "очень большой файл целых чисел" и порождают:  
(а) наибольшее число;  
(б) среднее арифметическое всех чисел;  
(в) набор тех же чисел, из которого исключены дубликаты;  
(г) количество различных целых чисел во входном наборе.  

Реализация представлена в файле [cmd/mapreduce/main.go](cmd/mapreduce/main.go)

#### Запуск программы
```bash
go run cmd/mapreduce/main.go very_large_integer_file.txt
```

### Проверка
Для проверки правильности выполнения программы можно воспользоваться следующим набором команд:

(а) `sort -n very_large_integer_file.txt | tail -n 1`  
(б) `awk '{sum += $1; count++} END {print sum/count}' very_large_integer_file.txt`  
(в) `sort -n very_large_integer_file.txt | uniq`  
(г) `sort -n very_large_integer_file.txt | uniq | wc -l`  
