`cargo run [encode [int|string]|decode] [filename]`

## encode

Zapisuje dane podanego typu to pliku. Dane są wczytywane z stdin. Może dodawać dane do istniejącego pliku.

## decode

Dekoduje dane z kolumny i wylicza zadane metryki.

## Format danych

Każda kolumna jest przechowywana w oddzielnym pliku. Dane w pliku są przechowywane w postaci niezależnie skompresowanych batchy.

```
file format:
magic: u32 (ISBD)
column type: u32
number of chunks: u64
=== start of chunks ===
for each chunk:
  chunk len in bytes: u64
  chunk len in numbers: u64
  === start of chunk data ===
  int chunk:
    vle encoded leader (smallest number in chunk)
    vle encoded data
  zstd compressed strings, for each string:
    string len: u32
    string: [u8]
```

## Struktura danych w pamięci

Iterator po batchach. Dane z pojedynczego batcha mogą zostać zebrane do wektora.
Każda kolumna będzie przetwarzana jako oddzielny iterator po batach. Z założenia każdy batch ma być na tyle mały, żeby dało się go wczytać do pamięci i zdekodować.

