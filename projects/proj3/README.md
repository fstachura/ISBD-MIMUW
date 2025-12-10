### Franciszek Stachura - baza danych

Program z bazą znajduje się w głównym katalogu repozytorium, w podkatalogu database. Makefile w tym katalogu posiada target `docker` który buduje obraz z tagiem `fstachura-database`. Kontener używa /data jako przestrzeni na pliki. 

`docker run --init -p 3000:3000 fstachura-database` - zalecane jest dodanie flagi `--init` aby móc zatrzymać kontener za pomocą ctrl+c.

Aby skompilować bazę lokalnie, można wykonać polecenie `make build` z poziomu katalogu `database` (potrzebny jest toolchain języka rust). Bazę następnie można uruchomić za pomocą `cargo run --release --bin api init data` które zainicjalizuje katalog danych bazy w katalogu `data`, oraz `cargo run --release --bin api serve data` które wystartuje serwer HTTP bazy.

Serwer HTTP domyślnie działa na porcie 3000 lokalnego hosta, adres oraz port można podać jako ostatni argument podkomendy `init`.

