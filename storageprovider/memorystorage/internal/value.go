package internal

import (
	"context"
	"errors"
	"sync"
)

type Value struct {
	mu        sync.Mutex
	v         string
	version   Version
	waiters   map[*waiter]struct{}
	refCount  int
	isDeleted bool
}

func (v *Value) Get() (string, Version) {
	v.mu.Lock()
	vv, version := v.v, v.version
	v.mu.Unlock()
	return vv, version
}

type WaitOptions struct {
	Interruption     <-chan struct{}
	InterruptedError error
}

func (v *Value) WaitFor(ctx context.Context, version Version, options WaitOptions, deleter ValueDeleter) (string, Version, error) {
	v.mu.Lock()
	if v.isDeleted {
		v.mu.Unlock()
		return "", 0, ErrValueIsDeleted
	}
	if v.version > version {
		vv, version := v.v, v.version
		v.mu.Unlock()
		return vv, version, nil
	}
	waiter := v.addWaiter()
	v.mu.Unlock()
	notification, err := waiter.WaitForNotification(ctx, options)
	if err != nil {
		v.mu.Lock()
		v.removeWaiter(waiter, deleter)
		v.mu.Unlock()
		return "", 0, err
	}
	return notification.Value, notification.LatestVersion, nil
}

func (v *Value) addWaiter() *waiter {
	waiter1 := new(waiter).Init()
	if v.waiters == nil {
		v.waiters = make(map[*waiter]struct{})
	}
	v.waiters[waiter1] = struct{}{}
	v.refCount++
	return waiter1
}

func (v *Value) removeWaiter(waiter *waiter, deleter ValueDeleter) {
	if _, ok := v.waiters[waiter]; !ok {
		return
	}
	delete(v.waiters, waiter)
	if len(v.waiters) == 0 {
		v.waiters = nil
	}
	v.refCount--
	if v.refCount == 0 {
		deleter()
		v.isDeleted = true
	}
}

func (v *Value) Set(vv string, latestVersion Version) error {
	if latestVersion == 0 {
		panic("invalid version")
	}
	v.mu.Lock()
	if v.isDeleted {
		v.mu.Unlock()
		return ErrValueIsDeleted
	}
	v.doSet(vv, latestVersion)
	waiters := v.resetWaiters()
	v.mu.Unlock()
	notification := notification{
		Value:         vv,
		LatestVersion: latestVersion,
	}
	for waiter := range waiters {
		waiter.Notify(notification)
	}
	return nil
}

func (v *Value) doSet(vv string, latestVersion Version) {
	v.v = vv
	wasNone := v.isNone()
	v.version = latestVersion
	if wasNone {
		v.refCount++
	}
}

func (v *Value) resetWaiters() map[*waiter]struct{} {
	waiters := v.waiters
	v.waiters = nil
	v.refCount -= len(waiters)
	return waiters
}

func (v *Value) Clear(deleter ValueDeleter) bool {
	v.mu.Lock()
	ok := v.doClear(deleter)
	v.mu.Unlock()
	return ok
}

func (v *Value) doClear(deleter ValueDeleter) bool {
	if v.isNone() {
		return false
	}
	v.v = ""
	v.version = 0
	v.refCount--
	if v.refCount == 0 {
		deleter()
		v.isDeleted = true
	}
	return true
}

type ValueDetails struct {
	V               string
	Version         Version
	NumberOfWaiters int
	RefCount        int
	IsDeleted       bool
}

func (v *Value) Inspect() ValueDetails {
	v.mu.Lock()
	details := ValueDetails{
		V:               v.v,
		Version:         v.version,
		NumberOfWaiters: len(v.waiters),
		RefCount:        v.refCount,
		IsDeleted:       v.isDeleted,
	}
	v.mu.Unlock()
	return details
}

func (v *Value) isNone() bool {
	return v.version == 0
}

type Version uint64

type ValueDeleter func()

var ErrValueIsDeleted = errors.New("internal: value is deleted")

type waiter struct {
	signal       chan struct{}
	notification notification
}

func (w *waiter) Init() *waiter {
	w.signal = make(chan struct{})
	return w
}

func (w *waiter) WaitForNotification(ctx context.Context, options WaitOptions) (notification, error) {
	select {
	case <-w.signal:
		return w.notification, nil
	case <-ctx.Done():
		return notification{}, ctx.Err()
	case <-options.Interruption:
		return notification{}, options.InterruptedError
	}
}

func (w *waiter) Notify(notification notification) {
	w.notification = notification
	close(w.signal)
}

type notification struct {
	Value         string
	LatestVersion Version
}
