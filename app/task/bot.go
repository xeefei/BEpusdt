package task

import (
	"context"

	"github.com/xeefei/bepusdt/app/bot"
)

func init() {
	register(task{callback: botStart})
}

func botStart(ctx context.Context) {
	bot.Start(ctx)
}
