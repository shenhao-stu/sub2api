package service

import "time"

// DeepSeek's official tariff uses Beijing time and treats Chinese statutory
// holidays as off-peak. Preserve the separate contracts of other resellers.
func deepseekGroupPeakMultiplierAt(at time.Time, group *Group) float64 {
	if group == nil || group.Platform != PlatformDeepseek || group.Name != "deepseek-official" {
		return deepseekPeakMultiplierAt(at)
	}
	local := at.In(time.FixedZone("Asia/Shanghai", 8*3600))
	spans, published := deepseekOfficialHolidays[local.Year()]
	if !published {
		return 1
	}
	day := 100*int(local.Month()) + local.Day()
	for _, span := range spans {
		if span[0] <= day && day <= span[1] {
			return 1
		}
	}
	return deepseekPeakMultiplierAt(at)
}

// Inclusive MMDD ranges from the State Council annual holiday calendar.
var deepseekOfficialHolidays = map[int][][2]int{
	2026: {{101, 103}, {215, 223}, {404, 406}, {501, 505}, {619, 621}, {925, 927}, {1001, 1007}},
	2027: {{101, 103}, {205, 213}, {403, 405}, {501, 505}, {609, 609}, {915, 915}, {1001, 1007}},
}
