all:	cakedb-driver

clean:
	rm -f cakedb-driver.o

fclean:	clean
	rm -f cakedb-driver

re:	fclean all

cakedb-driver:	cakedb-driver.o
	gcc cakedb-driver.o -o cakedb-driver

cakedb-driver.o: cakedb-driver.c
	gcc -c cakedb-driver.c -o cakedb-driver.o
