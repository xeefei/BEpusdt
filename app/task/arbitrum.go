package task

import (
	"context"
	"time"

	"github.com/smallnest/chanx"
	"github.com/xeefei/bepusdt/app/conf"
)

func arbitrumInit() {
	ctx := context.Background()
	arb := evm{
		Network:  conf.Arbitrum,
		Endpoint: conf.GetArbitrumRpcEndpoint(),
		Block: block{
			InitStartOffset: -50,
			ConfirmedOffset: 15,
		},
		blockScanQueue: chanx.NewUnboundedChan[evmBlock](ctx, 30),
	}

	register(task{callback: arb.blockDispatch})
	register(task{callback: arb.blockRoll, duration: time.Second * 45})
	register(task{callback: arb.tradeConfirmHandle, duration: time.Second * 45})
}
