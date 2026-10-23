// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
static _Atomic unsigned attempts, created, joined;
int pthread_create(pthread_t *thread, const pthread_attr_t *attr, void *(*start)(void *), void *arg) {
 unsigned attempt = atomic_fetch_add(&attempts, 1) + 1;
 const char *value = getenv("FAIL_THREAD_ATTEMPT");
 if (value && attempt == strtoul(value, NULL, 10)) { fprintf(stderr,"fault injection pthread_create #%u EAGAIN\n",attempt); return EAGAIN; }
 int (*original)(pthread_t *,const pthread_attr_t *,void *(*)(void *),void *) = dlsym(RTLD_NEXT,"pthread_create");
 int result = original(thread,attr,start,arg);
 if (result == 0) { atomic_fetch_add(&created,1); const char *delay=getenv("AFTER_CREATE_DELAY_US"); if (delay) usleep(strtoul(delay,NULL,10)); }
 return result;
}

int pthread_join(pthread_t thread, void **result) {
 int (*original)(pthread_t,void **) = dlsym(RTLD_NEXT,"pthread_join");
 int code = original(thread,result);
 if (code == 0) atomic_fetch_add(&joined,1);
 return code;
}
__attribute__((destructor)) static void verify_joins(void) {
 unsigned c=atomic_load(&created),j=atomic_load(&joined);
 fprintf(stderr,"pthread accounting: created=%u joined=%u\n",c,j);
 if (c != j) _Exit(97);
}
