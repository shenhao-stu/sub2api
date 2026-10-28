package service

// Only observed outputs may be billed; a request count or pricing switch is not delivery evidence.
func resolveBillableImageCount(reported int) int {
	return max(reported, 0)
}
