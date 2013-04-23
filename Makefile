all:
	gcc -Wall -O2 -DKISSDB_TEST -o kissdb-test kissdb.c

clean:
	rm -f kissdb-test *.o test.db
