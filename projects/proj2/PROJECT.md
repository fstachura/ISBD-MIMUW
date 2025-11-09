# Laboratorium 2 - Przygotowanie formatu pliku

- [Laboratorium 2 - Przygotowanie formatu pliku](#laboratorium-2---przygotowanie-formatu-pliku)
  - [Format pliku](#format-pliku)
  - [Serializator i deserializator](#serializator-i-deserializator)
  - [Reprezentacja w pamięci](#reprezentacja-w-pamięci)
  - [Struktura projektu](#struktura-projektu)
  - [Materiały do przeczytania](#materiały-do-przeczytania)


Celem laboratorium jest przygotowanie własnego formatu pliku, który będzie służyć do składowania danych na dysku.   

## Format pliku

Zaproponowany format powinien wspierać następujące funkcje:
* Przechowywanie dowolnej ilości kolumn
* Kolumny mają dwa dozwolone typy: 64-bitowa liczba całkowita ze znakiem oraz napis dowolnej długości (VARCHAR)
* Format pliku powinien wspierać kompresję (int64 powinien uzywac VLE oraz delta encoding, a VARCHAR kolumna ZTSD albo LZ4).
* Dane w pliku powinny być tabelaryczne (tzn. każda kolumna w pliku ma taka samą długość)

## Serializator i deserializator

Zaproponowany plik powinien być wykorzystywany w tworzonym analitycznym systemie DBMS jako źródło danych do przetwarzania.
Jako początek systemu przygotuj komponent odpowiedzialny za zapis oraz odczyt przygotowanego pliku.

## Reprezentacja w pamięci

Aby móc operować na wczytanych danych (lub je zapisać do pliku) potrzebna jest reprezentacja danych w pamięci.
Przygotuj typ reprezentujący kolumnowe dane gotowe do przetwarzania.
**Przygotuj strukturę danych w taki sposób, aby procesor mógł pracować na nich najbardziej wydajnie.**

## Struktura projektu
Jest to baza do zaliczenia kolejnych projektów. Proponuję wybrać technologię, która ułatwi pracę z danymi na tak niskim poziomie oraz będzie miała do dyspozycji dobrze znaną bibliotekę do implementacji REST API.

Do zaliczenia należy przygotować program, który wczytuje plik w zadanym formacie, wykonuje deserializację oraz wyznacza dwie metryki z wczytanych danych:
* Dla każdej kolumny o typie całkowitym oblicza średnią z wartości kolumny.
* Dla każdej kolumny o typie VARCHAR wyznacza liczbę wszystkich występujących znaków ASCII.

Warto także napisać program, który wykonuje operację serializacji.

## Materiały do przeczytania
* [Variable length int encoding](https://en.wikipedia.org/wiki/Variable-length_quantity)
* [Delta int encoding](https://en.wikipedia.org/wiki/Delta_encoding)
* [LZ4](https://github.com/lz4/lz4)
* [ZTSD](https://github.com/facebook/zstd)


TODO: 
1. Schematy testowania
2. Tor przetwarzania programu zaliczeniowego
3. Odnosnik Parquet 
