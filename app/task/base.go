package task

import (
	"context"
	"time"

	"github.com/smallnest/chanx"
	"github.com/v03413/bepusdt/app/conf"
)

func baseInit() {
	ctx := context.Background()
	base := evm{
		Network:  conf.Base,
		Endpoint: conf.GetBaseRpcEndpoint(),
		Block: block{
			InitStartOffset: -50,
			ConfirmedOffset: 15,
		},
		blockScanQueue: chanx.NewUnboundedChan[evmBlock](ctx, 30),
	}

	register(task{callback: base.blockDispatch})
	register(task{callback: base.blockRoll, duration: time.Second * 45})
	register(task{callback: base.tradeConfirmHandle, duration: time.Second * 45})
}
