NAME	=	cakedb-driver

OBJS	=	cakedb-driver.o		\
		network.o		\
		query.o			\
		range.o			\
		register.o		\
		store.o

DFLAGS	=	-D_GNU_SOURCE

CFLAGS	=	-Wall -Wstrict-prototypes -Wlong-long -Werror -std=c99 -pedantic

CC	=	cc


#####################################


all:	$(NAME)

clean:
	rm -f $(OBJS)

fclean:	clean
	rm -f $(NAME)
	rm -f *~ ./#*#

re:	fclean all


#####################################


cakedb-driver:	$(OBJS)
	${CC} ${CFLAGS} $(OBJS) -o $(NAME)

cakedb-driver.o:	cakedb-driver.c \
			cakedb-driver.h \
			register.h	\
			store.h		\
			query.h		\
			range.h
	$(CC) ${CFLAGS} ${DFLAGS} -c $< -o $@

network.o:	network.c	\
		network.h	\
		cakedb-driver.h
	$(CC) ${CFLAGS} ${DFLAGS} -c $< -o $@

query.o:	query.c		\
		query.h		\
		network.h	\
		register.h	\
		cakedb-driver.h
	$(CC) ${CFLAGS} ${DFLAGS} -c $< -o $@

range.o:	range.c		\
		range.h		\
		network.h	\
		register.h	\
		cakedb-driver.h
	$(CC) ${CFLAGS} ${DFLAGS} -c $< -o $@

register.o:	register.c	\
		register.h	\
		network.h	\
		cakedb-driver.h
	$(CC) ${CFLAGS} ${DFLAGS} -c $< -o $@

store.o:	store.c 	\
		store.h		\
		network.h	\
		register.h	\
		cakedb-driver.h
	$(CC) ${CFLAGS} ${DFLAGS} -c $< -o $@
