// Package requestlifecycle separates client cancellation from forced shutdown.
package requestlifecycle

import "context"

type forceCancellationKey struct{}

// WithForceCancellation attaches the server's final shutdown signal to a request.
func WithForceCancellation(ctx, force context.Context) context.Context {
	return context.WithValue(ctx, forceCancellationKey{}, force)
}

// WithoutClientCancel preserves request values and forced shutdown cancellation,
// allowing upstream usage collection to outlive an ordinary client disconnect.
func WithoutClientCancel(ctx context.Context) context.Context {
	values := context.WithoutCancel(ctx)
	force, ok := ctx.Value(forceCancellationKey{}).(context.Context)
	if !ok {
		return values
	}
	return upstreamContext{Context: force, values: values}
}

type upstreamContext struct {
	context.Context
	values context.Context
}

func (c upstreamContext) Value(key any) any { return c.values.Value(key) }
