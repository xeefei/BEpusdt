package task

import (
	"context"
	"time"

	"github.com/smallnest/chanx"
	"github.com/xeefei/bepusdt/app/conf"
)

func bscInit() {
	ctx := context.Background()
	bsc := evm{
		Network:  conf.Bsc,
		Endpoint: conf.GetBscRpcEndpoint(),
		Block: block{
			InitStartOffset: -50,
			ConfirmedOffset: 15,
		},
		blockScanQueue: chanx.NewUnboundedChan[evmBlock](ctx, 30),
	}

	register(task{callback: bsc.blockDispatch})
	register(task{callback: bsc.blockRoll, duration: time.Second * 45})
	register(task{callback: bsc.tradeConfirmHandle, duration: time.Second * 45})
}
