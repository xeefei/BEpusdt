package task

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcutil/base58"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/panjf2000/ants/v2"
	"github.com/shopspring/decimal"
	"github.com/smallnest/chanx"
	"github.com/spf13/cast"
	"github.com/xeefei/bepusdt/app/conf"
	"github.com/xeefei/bepusdt/app/log"
	"github.com/xeefei/bepusdt/app/model"
	"github.com/v03413/tronprotocol/api"
	"github.com/v03413/tronprotocol/core"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// --- 核心配置：API KEY ---
const TronApiKey = "0eccb2c4-ead5-4cb5-9a7b-5a5faff8e04a"

// --- 全局业务变量（完整保留） ---
var gasFreeUsdtTokenAddress = []byte{0xa6, 0x14, 0xf8, 0x03, 0xb6, 0xfd, 0x78, 0x09, 0x86, 0xa4, 0x2c, 0x78, 0xec, 0x9c, 0x7f, 0x77, 0xe6, 0xde, 0xd1, 0x3c}
var gasFreeOwnerAddress = []byte{0x41, 0x3b, 0x41, 0x50, 0x50, 0xb1, 0xe7, 0x9e, 0x38, 0x50, 0x7c, 0xb6, 0xe4, 0x8d, 0xac, 0xc2, 0x27, 0xaf, 0xfd, 0xd5, 0x0c}
var gasFreeContractAddress = []byte{0x41, 0x39, 0xdd, 0x12, 0xa5, 0x4e, 0x2b, 0xab, 0x7c, 0x82, 0xaa, 0x14, 0xa1, 0xe1, 0x58, 0xb3, 0x42, 0x63, 0xd2, 0xd5, 0x10}
var usdtTrc20ContractAddress = []byte{0x41, 0xa6, 0x14, 0xf8, 0x03, 0xb6, 0xfd, 0x78, 0x09, 0x86, 0xa4, 0x2c, 0x78, 0xec, 0x9c, 0x7f, 0x77, 0xe6, 0xde, 0xd1, 0x3c}
var usdcTrc20ContractAddress = []byte{0x41, 0x34, 0x87, 0xb6, 0x3d, 0x30, 0xb5, 0xb2, 0xc8, 0x7f, 0xb7, 0xff, 0xa8, 0xbc, 0xfa, 0xde, 0x38, 0xea, 0xac, 0x1a, 0xbe}

var trc20TokenDecimals = map[string]int32{
	model.OrderTradeTypeUsdtTrc20: conf.UsdtTronDecimals,
	model.OrderTradeTypeUsdcTrc20: conf.UsdcTronDecimals,
}

var grpcParams = grpc.ConnectParams{
	Backoff:           backoff.Config{BaseDelay: 1 * time.Second, MaxDelay: 30 * time.Second, Multiplier: 1.5},
	MinConnectTimeout: 1 * time.Minute,
}

type tron struct {
	blockConfirmedOffset int64
	blockInitStartOffset int64
	lastBlockNum         int64
	blockScanQueue       *chanx.UnboundedChan[int64]
	conn                 *grpc.ClientConn
}

var tr tron

func init() {
	tr = newTron()
	register(task{duration: time.Second * 45, callback: tr.blockDispatch})
	register(task{duration: time.Second * 45, callback: tr.blockRoll})
	register(task{duration: time.Second * 45, callback: tr.tradeConfirmHandle})
}

func newTron() tron {
	return tron{
		blockConfirmedOffset: 30,
		blockInitStartOffset: -50,
		lastBlockNum:         0,
		blockScanQueue:       chanx.NewUnboundedChan[int64](context.Background(), 50),
	}
}

// 增加 API Key 的 Context
func (t *tron) getContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	ctx = metadata.AppendToOutgoingContext(ctx, "TRON-PRO-API-KEY", TronApiKey)
	return ctx, cancel
}

func (t *tron) getConn() (*grpc.ClientConn, error) {
	if t.conn != nil {
		state := t.conn.GetState().String()
		if state != "SHUTDOWN" && state != "TRANSIENT_FAILURE" {
			return t.conn, nil
		}
		_ = t.conn.Close()
	}
	conn, err := grpc.NewClient(conf.GetTronGrpcNode(), grpc.WithConnectParams(grpcParams), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	t.conn = conn
	return t.conn, nil
}

func (t *tron) blockRoll(context.Context) {
	if t.rollBreak() { return }
	conn, err := t.getConn()
	if err != nil { return }
	var client = api.NewWalletClient(conn)
	ctx, cancel := t.getContext()
	defer cancel()
	block, err1 := client.GetNowBlock2(ctx, &api.EmptyMessage{})
	if err1 != nil { return }
	var now = block.BlockHeader.RawData.Number
	if conf.GetTradeIsConfirmed() { now = now - t.blockConfirmedOffset }
	if now-t.lastBlockNum > conf.BlockHeightMaxDiff {
		t.blockInitOffset(now)
		t.lastBlockNum = now - 1
	}
	if now <= t.lastBlockNum { return }
	for n := t.lastBlockNum + 1; n <= now; n++ {
		t.blockScanQueue.In <- n
	}
	t.lastBlockNum = now
}

func (t *tron) blockDispatch(context.Context) {
	p, err := ants.NewPoolWithFunc(2, t.blockParse)
	if err != nil { return }
	defer p.Release()
	for n := range t.blockScanQueue.Out {
		if err := p.Invoke(n); err != nil {
			time.AfterFunc(time.Second*5, func() { t.blockScanQueue.In <- n })
		}
	}
}

func (t *tron) blockParse(n any) {
	var num = n.(int64)
	conn, err := t.getConn()
	if err != nil { return }
	var client = api.NewWalletClient(conn)
	conf.SetBlockTotal(conf.Tron)
	ctx, cancel := t.getContext()
	defer cancel()
	bok, err2 := client.GetBlockByNum2(ctx, &api.NumberMessage{Num: num})
	if err2 != nil {
		conf.SetBlockFail(conf.Tron)
		time.AfterFunc(time.Second*5, func() { t.blockScanQueue.In <- num })
		log.Warn(fmt.Sprintf("Tron 区块 %d 获取失败: %v", num, err2))
		return
	}

	var transfers = make([]transfer, 0)
	var timestamp = time.UnixMilli(bok.GetBlockHeader().GetRawData().GetTimestamp())
	
	for _, trans := range bok.GetTransactions() {
		if !trans.Result.Result { continue }
		var itm = trans.GetTransaction()
		var id = hex.EncodeToString(trans.Txid)
		for _, contract := range itm.GetRawData().GetContract() {
			// 1. TRX 转账
			if contract.GetType() == core.Transaction_Contract_TransferContract {
				var foo = &core.TransferContract{}
				if err := contract.GetParameter().UnmarshalTo(foo); err == nil {
					transfers = append(transfers, transfer{
						Network: conf.Tron, TxHash: id, BlockNum: num, Timestamp: timestamp, TradeType: model.OrderTradeTypeTronTrx,
						Amount: decimal.NewFromBigInt(new(big.Int).SetInt64(foo.Amount), -6),
						FromAddress: t.base58CheckEncode(foo.OwnerAddress), RecvAddress: t.base58CheckEncode(foo.ToAddress),
					})
				}
			}
			// 2. TRC20 及特殊免手续费逻辑
			if contract.GetType() == core.Transaction_Contract_TriggerSmartContract {
				var foo = &core.TriggerSmartContract{}
				if err := contract.GetParameter().UnmarshalTo(foo); err != nil { continue }
				data := foo.GetData()
				if len(data) < 4 { continue }

				// 处理特殊免手续费逻辑 (permitTransfer)
				if bytes.Equal(data[:4], []byte{0x6f, 0x21, 0xb8, 0x98}) {
					from, recv, amount := t.gasFreePermitTransfer(data)
					if amount != nil {
						transfers = append(transfers, transfer{
							Network: conf.Tron, TxHash: id, BlockNum: num, Timestamp: timestamp,
							TradeType: model.OrderTradeTypeUsdtTrc20, Amount: decimal.NewFromBigInt(amount, -6),
							FromAddress: from, RecvAddress: recv,
						})
					}
					continue
				}

				// 匹配合约地址
				var tradeType = ""
				if bytes.Equal(foo.GetContractAddress(), usdtTrc20ContractAddress) { tradeType = model.OrderTradeTypeUsdtTrc20
				} else if bytes.Equal(foo.GetContractAddress(), usdcTrc20ContractAddress) { tradeType = model.OrderTradeTypeUsdcTrc20 }
				exp, ok := trc20TokenDecimals[tradeType]
				if !ok { continue }

				// 方法解析
				method := data[:4]
				if bytes.Equal(method, []byte{0xa9, 0x05, 0x9c, 0xbb}) { // transfer
					recv, amount := t.parseTrc20ContractTransfer(data)
					if amount != nil {
						transfers = append(transfers, transfer{
							Network: conf.Tron, TxHash: id, BlockNum: num, Timestamp: timestamp, TradeType: tradeType,
							Amount: decimal.NewFromBigInt(amount, exp), FromAddress: t.base58CheckEncode(foo.OwnerAddress), RecvAddress: recv,
						})
					}
				} else if bytes.Equal(method, []byte{0x23, 0xba, 0xae, 0xae}) { // transferFrom
					from, recv, amount := t.parseTrc20ContractTransferFrom(data)
					if amount != nil {
						transfers = append(transfers, transfer{
							Network: conf.Tron, TxHash: id, BlockNum: num, Timestamp: timestamp, TradeType: tradeType,
							Amount: decimal.NewFromBigInt(amount, exp), FromAddress: from, RecvAddress: recv,
						})
					}
				}
			}
		}
	}
	if len(transfers) > 0 { transferQueue.In <- transfers }
	log.Info("Tron 区块扫描完成", num, "成功率:", conf.GetBlockSuccRate(conf.Tron))
}

// --- 以下所有业务函数均已保留并修正 ---

func (t *tron) blockInitOffset(now int64) {
	if now == 0 || t.lastBlockNum != 0 { return }
	go func() {
		ticker := time.NewTicker(time.Millisecond * 500)
		defer ticker.Stop()
		endOffset := now + t.blockInitStartOffset
		for num := now; num >= endOffset; num-- {
			if t.rollBreak() { return }
			t.blockScanQueue.In <- num
			<-ticker.C
		}
	}()
}

func (t *tron) parseTrc20ContractTransfer(data []byte) (string, *big.Int) {
	if len(data) != 68 { return "", nil }
	receiver := t.base58CheckEncode(append([]byte{0x41}, data[16:36]...))
	amount := big.NewInt(0).SetBytes(data[36:68])
	return receiver, amount
}

func (t *tron) parseTrc20ContractTransferFrom(data []byte) (string, string, *big.Int) {
	if len(data) != 100 { return "", "", nil }
	from := t.base58CheckEncode(append([]byte{0x41}, data[16:36]...))
	to := t.base58CheckEncode(append([]byte{0x41}, data[48:68]...))
	amount := big.NewInt(0).SetBytes(data[68:100])
	return from, to, amount
}

func (t *tron) gasFreePermitTransfer(data []byte) (string, string, *big.Int) {
	if len(data) != 420 || !bytes.Equal(data[:4], []byte{0x6f, 0x21, 0xb8, 0x98}) { return "", "", nil }
	// 校验是否为指定的免手续费 USDT 合约
	if !bytes.Equal(data[16:36], gasFreeUsdtTokenAddress) { return "", "", nil }
	user := t.base58CheckEncode(append([]byte{0x41}, data[48:68]...))
	receiver := t.base58CheckEncode(append([]byte{0x41}, data[80:100]...))
	amount := big.NewInt(0).SetBytes(data[100:132])
	return user, receiver, amount
}

func (t *tron) tradeConfirmHandle(ctx context.Context) {
	var orders = getConfirmingOrders([]string{model.OrderTradeTypeTronTrx, model.OrderTradeTypeUsdtTrc20, model.OrderTradeTypeUsdcTrc20})
	if len(orders) == 0 { return }
	conn, err := t.getConn()
	if err != nil { return }
	var client = api.NewWalletClient(conn)
	var wg sync.WaitGroup
	for _, order := range orders {
		wg.Add(1)
		go func(o model.TradeOrders) {
			defer wg.Done()
			idBytes, _ := hex.DecodeString(o.TradeHash)
			tCtx, cancel := t.getContext()
			defer cancel()
			if o.TradeType == model.OrderTradeTypeTronTrx {
				trans, err := client.GetTransactionById(tCtx, &api.BytesMessage{Value: idBytes})
				if err == nil && len(trans.GetRet()) > 0 && trans.GetRet()[0].ContractRet == core.Transaction_Result_SUCCESS {
					markFinalConfirmed(o)
				}
			} else {
				info, err := client.GetTransactionInfoById(tCtx, &api.BytesMessage{Value: idBytes})
				if err == nil && info.GetReceipt().GetResult() == core.Transaction_Result_SUCCESS {
					markFinalConfirmed(o)
				}
			}
		}(order)
	}
	wg.Wait()
}

func (t *tron) base58CheckEncode(input []byte) string {
	checksum := chainhash.DoubleHashB(input)
	input = append(input, checksum[:4]...)
	return base58.Encode(input)
}

func (t *tron) rollBreak() bool {
	var count int64 = 0
	trade := []string{model.OrderTradeTypeTronTrx, model.OrderTradeTypeUsdtTrc20, model.OrderTradeTypeUsdcTrc20}
	model.DB.Model(&model.TradeOrders{}).Where("status = ? and trade_type in (?)", model.OrderStatusWaiting, trade).Count(&count)
	if count > 0 { return false }
	model.DB.Model(&model.WalletAddress{}).Where("other_notify = ? and trade_type in (?)", model.OtherNotifyEnable, trade).Count(&count)
	return count <= 0
}
