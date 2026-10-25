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

/* Test-only source-bound accounting, armed around the first native build only.
 * The exact expected count comes from external normal-wheel build provenance.
 * Descriptor number AND device/inode are retained; dup(receiver) is not exempt.
 */
#include <stdint.h>
#include <sys/socket.h>
#include <sys/stat.h>
static _Atomic unsigned tracking_pairs, observed_pairs, invalid_pairs;
static int global_pair_fds[2] = {-1, -1};
static uint64_t global_pair_inodes[2], global_pair_devices[2];

void startup_track_pairs(unsigned armed) { atomic_store(&tracking_pairs, armed); }
unsigned startup_pair_count(void) { return atomic_load(&observed_pairs); }
unsigned startup_invalid_pairs(void) { return atomic_load(&invalid_pairs); }
int startup_pair_fd(unsigned index) { return index < 2 ? global_pair_fds[index] : -1; }
uint64_t startup_pair_inode(unsigned index) { return index < 2 ? global_pair_inodes[index] : 0; }
uint64_t startup_pair_device(unsigned index) { return index < 2 ? global_pair_devices[index] : 0; }

int socketpair(int domain, int type, int protocol, int pair[2]) {
 int (*original)(int, int, int, int[2]) = dlsym(RTLD_NEXT, "socketpair");
 if (!original) _Exit(98);
 int result = original(domain, type, protocol, pair);
 if (result == 0 && atomic_load(&tracking_pairs)) {
  struct stat identities[2];
  int valid = fstat(pair[0], &identities[0]) == 0 && fstat(pair[1], &identities[1]) == 0;
  if (!valid || domain != AF_UNIX || type != (SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK) || protocol != 0)
   atomic_fetch_add(&invalid_pairs, 1);
  if (atomic_fetch_add(&observed_pairs, 1) == 0 && valid) {
   for (unsigned index = 0; index < 2; index++) {
    global_pair_fds[index] = pair[index];
    global_pair_inodes[index] = identities[index].st_ino;
    global_pair_devices[index] = identities[index].st_dev;
   }
  }
 }
 return result;
}
