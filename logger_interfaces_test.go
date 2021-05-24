/*
 * Author: Aniruddha Maru
 */

package hedwig

import (
	"context"

	"github.com/sirupsen/logrus"
)

// compile time check for LogrusGetLoggerFunc
var _ = GetLoggerFunc(LogrusGetLoggerFunc(func(_ context.Context) *logrus.Entry {
	return logrus.NewEntry(logrus.StandardLogger())
}))
