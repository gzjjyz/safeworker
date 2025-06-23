/**
 * @Author: zjj
 * @Date: 2025/6/23
 * @Desc:
**/

package safeworker

type DescriptionOption func(*description)

func WithDescriptionOptionSkipAlarm() DescriptionOption {
	return func(d *description) {
		d.skipAlarm = true
	}
}
