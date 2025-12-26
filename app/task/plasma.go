package task

import (
	"context"
	"time"

	"github.com/smallnest/chanx"
	"github.com/v03413/bepusdt/app/conf"
)

func plasmaInit() {
	ctx := context.Background()
	xpl := evm{
		Network:  conf.Plasma,
		Endpoint: conf.GetPlasmaRpcEndpoint(),
		Block: block{
			InitStartOffset: -50,
			ConfirmedOffset: 15,
		},
		blockScanQueue: chanx.NewUnboundedChan[evmBlock](ctx, 30),
	}

	register(task{callback: xpl.blockDispatch})
	register(task{callback: xpl.blockRoll, duration: time.Second * 45})
	register(task{callback: xpl.tradeConfirmHandle, duration: time.Second * 45})
}
