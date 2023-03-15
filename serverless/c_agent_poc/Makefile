all: clean ld_preload ld_preload_init customer_function 

customer_function:
	gcc -o customer_function user_main.c

ld_preload:
	gcc -o libddpreload.so -fPIC -shared -ldl main.c

ld_preload_init:
	gcc -o libddpreload_init.so -fPIC -shared -ldl main_init.c

clean:
	rm -f customer_function libddpreload.so
