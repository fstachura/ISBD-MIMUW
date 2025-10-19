#include <bits/time.h>
#include <stdio.h>
#include <unistd.h>
#include <memory.h>
#include <sys/mman.h>
#include <malloc.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <time.h>
#include "crc64table.h"

const char SYSCALL_READ_STR[]   = "read";
const char SYSCALL_MMAP_STR[]   = "mmap";

#define SYSCALL_READ    1
#define SYSCALL_MMAP    2

const char ORDER_SEQ_STR[]      = "seq";
const char ORDER_RANDOM_STR[]   = "random";

#define ORDER_SEQ       1
#define ORDER_RANDOM    2

typedef void* iterator;

struct iterator_ctl {
    void* (*init)(long size);
    int (*free)(void* data);
    long (*advance)(void* data);
};

struct seq_iterator {
    long i;
    long n;
};

void* init_seq_iterator(long size) {
    struct seq_iterator* data;
    void *it = malloc(sizeof(struct seq_iterator));
    if (it == NULL)
        return NULL;

    data = (struct seq_iterator*)it;
    data->i = 0;
    data->n = size;
    return it;
}

int free_seq_iterator(void* it) {
    free(it);
    return 0;
}

long advance_seq_iterator(void* it) {
    struct seq_iterator* data = (struct seq_iterator*)it;

    long i = data->i;
    if (data->i < data->n)
        data->i++;

    return i;
}

const struct iterator_ctl seq_iterator_ctl = {
    .init = init_seq_iterator,
    .free = free_seq_iterator,
    .advance = advance_seq_iterator,
};


struct random_iterator {
    long i;
    long* arr;
};

void* init_random_iterator(long size) {
    struct random_iterator* data;
    void* it = malloc(sizeof(struct seq_iterator));
    if (it == NULL)
        return NULL;

    data = (struct random_iterator*)it;
    data->i = 0;
    data->arr = malloc(sizeof(long)*size);
    if (data->arr == NULL) {
        free(it);
        return NULL;
    }

    assert(RAND_MAX >= 0x7fffffff);
    for (long i=0; i < size-1; i++) {
        long n = (((uint64_t)rand()) | (((uint64_t)rand()) << 31) | ((((uint64_t)rand()) & 1) << 62)) & 0x7fffffffffffffff;
        long j = i + n / (0x7fffffffffffffff / (size-i)+1);
        //printf("%ld %ld %ld %lx\n", i, size-1, j, n);
        long el = data->arr[i];
        data->arr[i] = j;
        data->arr[j] = i;
    }

    return it;
}

int free_random_iterator(void* it) {
    struct random_iterator* data = (struct random_iterator*)it;
    free(data->arr);
    free(it);
    return 0;
}

long advance_random_iterator(void* it) {
    struct seq_iterator* data = (struct seq_iterator*)it;

    unsigned int i = data->i;
    if (data->i < data->n)
        data->i++;

    return i;
}

const struct iterator_ctl random_iterator_ctl = {
    .init = init_random_iterator,
    .free = free_random_iterator,
    .advance = advance_random_iterator,
};


long get_file_size(int fd) {
    long file_size;

    if (lseek(fd, 0, SEEK_END) < 0) {
        perror("failed to lseek");
        return -1;
    }

    file_size = lseek(fd, 0, SEEK_CUR);
    if (file_size < 0) {
        perror("failed to lseek");
        return -1;
    }

    return file_size;
}

// taken from Linux v6.17.2, lib/crc/crc64-main.c
// Copyright 2018 SUSE Linux.
// Author: Coly Li <colyli@suse.de>
uint64_t crc64_hash(uint64_t hash, uint8_t* data, uint64_t len) {
    while(len--)
        hash = (hash << 8) ^ crc64table[(hash >> 56) ^ *data++];
    return hash;
}

struct test_params {
    int fd;
    long file_size;
    long block_size;
    long blocks;
    iterator it;
    const struct iterator_ctl* it_ctl;
    long bytes_read;
    uint64_t hash;
    struct timespec begin;
    struct timespec end;
};

int test_with_read(struct test_params* params) {
    int result = 0;
    long block_size = params->block_size;
    long remaining_size = params->file_size;

    void* block = malloc(params->block_size);
    if (block == NULL) {
        fprintf(stderr, "failed to malloc block\n");
        return -1;
    }

    long i = params->it_ctl->advance(params->it);
    if (clock_gettime(CLOCK_MONOTONIC, &params->begin) != 0) {
        perror("failed to read begin clock");
        result = -1;
        goto end;
    }

    while (remaining_size > 0) {
        result = lseek(params->fd, i*block_size, SEEK_SET);
        if (result < 0) {
            perror("failed to seek");
            goto end;
        }

        long to_read = remaining_size > block_size ? block_size : remaining_size;
        long read_result = read(params->fd, block, to_read);
        if (read_result <= 0) {
            printf("%ld %ld\n", i, i*block_size);
            perror("failed to read");
            result = -1;
            goto end;
        }

        params->hash = crc64_hash(params->hash, block, read_result);
        params->bytes_read += read_result;

        long ni = params->it_ctl->advance(params->it);
        if (ni == i) {
            goto end;
        }

        i = ni;
        remaining_size -= read_result;
    }

end:
    if(clock_gettime(CLOCK_MONOTONIC, &params->end) != 0) {
        perror("failed to read end clock");
        result = -1;
    }

    free(block);
    return result;
}

int test_with_mmap(struct test_params* params) {
    int result = 0;
    void* data = mmap(NULL, params->file_size, PROT_READ, MAP_PRIVATE, params->fd, 0);
    if (data == MAP_FAILED) {
        perror("failed to mmap\n");
        return -1;
    }

    long block_size = params->block_size;
    long remaining_size = params->file_size;
    long i = params->it_ctl->advance(params->it);

    if (clock_gettime(CLOCK_MONOTONIC, &params->begin) != 0) {
        perror("failed to read begin clock");
        result = -1;
        goto end;
    }

    while (remaining_size > 0) {
        long to_read = remaining_size > block_size ? block_size : remaining_size;

        params->hash = crc64_hash(params->hash, data + params->bytes_read, to_read);

        params->bytes_read += to_read;
        long ni = params->it_ctl->advance(params->it);
        if (ni == i)
            goto end;

        i = ni;
        remaining_size -= to_read;
    }

end:
    if(clock_gettime(CLOCK_MONOTONIC, &params->end) != 0) {
        perror("failed to read end clock");
        result = -1;
    }

    munmap(data, params->file_size);
    return result;
}

int init_srand() {
    uint64_t val;
    int result = 0;
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd <= 0) {
        perror("failed to open urandom");
        return -1;
    }

    if (read(fd, &val, sizeof(val)) != sizeof(val)) {
        perror("failed to read srand val");
        result = -1;
    } else {
        srand(val);
    }

    assert(close(fd) == 0);
    return result;
}

int main(int argc, char** argv) {
    int result, err;
    int fd;
    long block_size;
    unsigned int syscall, order;

    if (argc != 5) {
        printf("usage: %s file_name block_len syscall order\n", argv[0]);
        printf("valid syscall modes: %s, %s\n", SYSCALL_READ_STR, SYSCALL_MMAP_STR);
        printf("valid order modes: %s, %s\n", ORDER_SEQ_STR, ORDER_RANDOM_STR);
        return -1;
    }

    if (init_srand() != 0) {
        fprintf(stderr, "failed to init srand\n");
        return -1;
    }

    fd = open(argv[1], O_RDONLY);
    if (fd < 0) {
        perror("failed to open file\n");
        return -1;
    }

    result = sscanf(argv[2], "%ld", &block_size);
    if (result != 1) {
        fprintf(stderr, "failed to parse blocksize %d\n", result);
        return -1;
    }

    const struct iterator_ctl* it_ctl;
    if (strcmp(argv[4], ORDER_SEQ_STR) == 0) {
        printf("order %s\n", ORDER_SEQ_STR);
        it_ctl = &seq_iterator_ctl;
    } else if (strcmp(argv[4], ORDER_RANDOM_STR) == 0) {
        it_ctl = &random_iterator_ctl;
        printf("order %s\n", ORDER_RANDOM_STR);
    } else {
        fprintf(stderr, "invalid order. valid options: %s, %s\n", ORDER_SEQ_STR, ORDER_RANDOM_STR);
        return -1;
    }

    long file_size = get_file_size(fd);
    if (file_size < 0) {
        fprintf(stderr, "failed to get file size\n");
        return -1;
    }

    long blocks = (file_size / block_size) + (file_size%block_size != 0 ? 1 : 0);
    iterator it = it_ctl->init(blocks);
    if (it == NULL) {
        fprintf(stderr, "failed to init iterator\n");
        err = -1;
        goto failed;
    }

    struct test_params params;
    params.block_size = block_size;
    params.blocks = blocks;
    params.file_size = file_size;
    params.fd = fd;
    params.it = it;
    params.it_ctl = it_ctl;
    params.bytes_read = 0;

    if (strcmp(argv[3], SYSCALL_READ_STR) == 0) {
        printf("syscall %s\n", SYSCALL_READ_STR);
        result = test_with_read(&params);
    } else if (strcmp(argv[3], SYSCALL_MMAP_STR) == 0) {
        printf("syscall %s\n", SYSCALL_MMAP_STR);
        result = test_with_mmap(&params);
    } else {
        fprintf(stderr, "invalid syscall. valid options: %s, %s\n", SYSCALL_READ_STR, SYSCALL_MMAP_STR);
        err = -1;
        goto failed;
    }

    printf("file_size %ld\n", file_size);
    printf("bytes_read %ld\n", params.bytes_read);
    printf("block_size %ld\n", block_size);
    printf("blocks %ld\n", blocks);
    printf("hash %lx\n", params.hash);

    if (result < 0) {
        fprintf(stderr, "failed to finish test\n");
        err = result;
        goto failed;
    }

    return 0;

failed:
    assert(it_ctl->free(it) == 0);
    close(fd);
    return err;
}
