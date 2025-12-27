package task

import (
	"bytes"
	"context"
	"encoding/json" // 新增：用于序列化地址数组
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/shopspring/decimal"
	"github.com/smallnest/chanx"
	"github.com/tidwall/gjson"
	"github.com/xeefei/bepusdt/app/conf"
	"github.com/xeefei/bepusdt/app/help"
	"github.com/xeefei/bepusdt/app/log"
	"github.com/xeefei/bepusdt/app/model"
)

const (
	blockParseMaxNum = 10 // 每次解析区块的最大数量
	evmTransferEvent = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)

var chainBlockNum sync.Map
var contractMap = map[string]string{
	conf.UsdtXlayer:   model.OrderTradeTypeUsdtXlayer,
	conf.UsdtBep20:    model.OrderTradeTypeUsdtBep20,
	conf.UsdtPolygon:  model.OrderTradeTypeUsdtPolygon,
	conf.UsdtArbitrum: model.OrderTradeTypeUsdtArbitrum,
	conf.UsdtPlasma:   model.OrderTradeTypeUsdtPlasma,
	conf.UsdtErc20:    model.OrderTradeTypeUsdtErc20,
	conf.UsdcErc20:    model.OrderTradeTypeUsdcErc20,
	conf.UsdcPolygon:  model.OrderTradeTypeUsdcPolygon,
	conf.UsdcXlayer:   model.OrderTradeTypeUsdcXlayer,
	conf.UsdcArbitrum: model.OrderTradeTypeUsdcArbitrum,
	conf.UsdcBep20:    model.OrderTradeTypeUsdcBep20,
	conf.UsdcBase:     model.OrderTradeTypeUsdcBase,
}

var networkTokenMap = map[string][]string{
	conf.Bsc:      {model.OrderTradeTypeUsdtBep20, model.OrderTradeTypeUsdcBep20},
	conf.Xlayer:   {model.OrderTradeTypeUsdtXlayer, model.OrderTradeTypeUsdcXlayer},
	conf.Polygon:  {model.OrderTradeTypeUsdtPolygon, model.OrderTradeTypeUsdcPolygon},
	conf.Arbitrum: {model.OrderTradeTypeUsdtArbitrum, model.OrderTradeTypeUsdcArbitrum},
	conf.Plasma:   {model.OrderTradeTypeUsdtPlasma},
	conf.Ethereum: {model.OrderTradeTypeUsdtErc20, model.OrderTradeTypeUsdcErc20},
	conf.Base:     {model.OrderTradeTypeUsdcBase},
	conf.Solana:   {model.OrderTradeTypeUsdtSolana, model.OrderTradeTypeUsdcSolana},
	conf.Aptos:    {model.OrderTradeTypeUsdtAptos, model.OrderTradeTypeUsdcAptos},
}

var client = &http.Client{Timeout: time.Second * 30}
var decimals = map[string]int32{
	conf.UsdtXlayer:   conf.UsdtXlayerDecimals,
	conf.UsdtBep20:    conf.UsdtBscDecimals,
	conf.UsdtPolygon:  conf.UsdtPolygonDecimals,
	conf.UsdtArbitrum: conf.UsdtArbitrumDecimals,
	conf.UsdtPlasma:   conf.UsdtPlasmaDecimals,
	conf.UsdtErc20:    conf.UsdtEthDecimals,
	conf.UsdcErc20:    conf.UsdcEthDecimals,
	conf.UsdcPolygon:  conf.UsdcPolygonDecimals,
	conf.UsdcXlayer:   conf.UsdcXlayerDecimals,
	conf.UsdcArbitrum: conf.UsdcArbitrumDecimals,
	conf.UsdcBep20:    conf.UsdcBscDecimals,
	conf.UsdcBase:     conf.UsdcBaseDecimals,
	conf.UsdcAptos:    conf.UsdcAptosDecimals,
	conf.UsdtAptos:    conf.UsdtAptosDecimals,
}

type block struct {
	InitStartOffset int64 // 首次偏移量
	RollDelayOffset int64 // 延迟偏移量
	ConfirmedOffset int64 // 确认偏移量
}

type evm struct {
	Network        string
	Endpoint       string
	Block          block
	blockScanQueue *chanx.UnboundedChan[evmBlock]
}

type evmBlock struct {
	From int64
	To   int64
}

// blockRoll 定时检查最新高度并将任务入队
func (e *evm) blockRoll(ctx context.Context) {
	// 【优化】添加日志，确认函数是否执行，排查 BSC “静默”问题
	// 如果觉得太吵，可以将 log.Info 改为 log.Debug
	if rollBreak(e.Network) {
		log.Debug("当前网络无待处理订单，暂停扫块:", e.Network) // 可选开启调试
		return
	}

	post := []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)
	req, err := http.NewRequestWithContext(ctx, "POST", e.Endpoint, bytes.NewBuffer(post))
	if err != nil {
		log.Warn("创建高度请求失败:", err, " Network:", e.Network)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Warn("发送高度请求失败:", err, " Network:", e.Network)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Warn("读取高度响应失败:", err)
		return
	}

	var res = gjson.ParseBytes(body)
	if !res.Get("result").Exists() {
		log.Warn("节点返回格式错误:", string(body), " Network:", e.Network)
		return
	}

	var now = help.HexStr2Int(res.Get("result").String()).Int64() - e.Block.RollDelayOffset
	if now <= 0 {
		return
	}

	if conf.GetTradeIsConfirmed() {
		now = now - e.Block.ConfirmedOffset
	}

	var lastBlockNumber int64
	if v, ok := chainBlockNum.Load(e.Network); ok {
		lastBlockNumber = v.(int64)
	}

	// 如果断档太久，则从初始偏移量开始扫
	if now-lastBlockNumber > conf.BlockHeightMaxDiff {
		lastBlockNumber = e.blockInitOffset(now, e.Block.InitStartOffset) - 1
	}

	chainBlockNum.Store(e.Network, now)
	
	// 【优化】增加高度检查日志，如果 now <= lastBlockNumber，说明没有新块，自然没有后续日志
	if now <= lastBlockNumber {
		// log.Info("区块高度未更新:", e.Network, " 当前:", now, " 上次:", lastBlockNumber)
		return
	}

	log.Info("发现新区块范围:", e.Network, " From:", lastBlockNumber+1, " To:", now)

	// 将需要扫描的区块范围拆分任务入队
	for from := lastBlockNumber + 1; from <= now; from += blockParseMaxNum {
		to := from + blockParseMaxNum - 1
		if to > now {
			to = now
		}
		e.blockScanQueue.In <- evmBlock{From: from, To: to}
	}
}

// blockInitOffset 初始化扫描偏移量
func (e *evm) blockInitOffset(now, offset int64) int64 {
	log.Info("初始化/重置区块扫描进度:", e.Network, " 当前高度:", now)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for b := now; b > now+offset; b -= blockParseMaxNum {
			if rollBreak(e.Network) {
				return
			}
			e.blockScanQueue.In <- evmBlock{From: b - blockParseMaxNum + 1, To: b}
			<-ticker.C
		}
	}()
	return now
}

// blockDispatch 调度器：负责消费队列并启动协程池处理
func (e *evm) blockDispatch(ctx context.Context) {
	// 降低并发数至1或2，减轻节点压力，防止 limit exceeded
	p, err := ants.NewPoolWithFunc(1, e.getBlockByNumber)
	if err != nil {
		panic(err)
		return
	}
	defer p.Release()

	for {
		select {
		case <-ctx.Done():
			return
		case n := <-e.blockScanQueue.Out:
			if err := p.Invoke(n); err != nil {
				// 发生错误时，稍等一下再重新入队，防止瞬间死循环
				time.AfterFunc(time.Second*5, func() {
					e.blockScanQueue.In <- n
				})
				log.Warn("任务分发失败:", err)
			}
		}
	}
}

// getBlockByNumber 获取区块详情并解析日志
func (e *evm) getBlockByNumber(a any) {
	b, ok := a.(evmBlock)
	if !ok {
		log.Warn("evmBlockParse 类型错误")
		return
	}

	items := make([]string, 0)
	for i := b.From; i <= b.To; i++ {
		items = append(items, fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x%x",false],"id":%d}`, i, i))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", e.Endpoint, bytes.NewBuffer([]byte(fmt.Sprintf(`[%s]`, strings.Join(items, ",")))))
	if err != nil {
		log.Warn("创建Batch请求失败:", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		conf.SetBlockFail(e.Network)
		// 失败重试：延迟入队
		time.AfterFunc(time.Second*8, func() { e.blockScanQueue.In <- b })
		log.Warn("eth_getBlockByNumber 网络错误:", err, " Network:", e.Network)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Warn("读取区块响应失败:", err)
		return
	}

	timestamp := make(map[string]time.Time)
	for _, itm := range gjson.ParseBytes(body).Array() {
		if itm.Get("error").Exists() {
			conf.SetBlockFail(e.Network)
			time.AfterFunc(time.Second*8, func() { e.blockScanQueue.In <- b })
			log.Warn(fmt.Sprintf("%s 节点返回错误: %s", e.Network, itm.Get("error").String()))
			return
		}
		timestamp[itm.Get("result.number").String()] = time.Unix(help.HexStr2Int(itm.Get("result.timestamp").String()).Int64(), 0)
	}

	// 调用解析日志逻辑
	transfers, err := e.parseBlockTransfer(b, timestamp)
	if err != nil {
		conf.SetBlockFail(e.Network)
		time.AfterFunc(time.Second*8, func() { e.blockScanQueue.In <- b })
		log.Warn("日志解析过程报错:", err, " Network:", e.Network)
		return
	}

	if len(transfers) > 0 {
		transferQueue.In <- transfers
	}

	log.Info("区块扫描完成", b, "成功率:", conf.GetBlockSuccRate(e.Network), "网络:", e.Network)
}

// parseBlockTransfer 核心优化：获取特定合约日志
func (e *evm) parseBlockTransfer(b evmBlock, timestamp map[string]time.Time) ([]transfer, error) {
	transfers := make([]transfer, 0)

	// 1. 获取当前网络需要监听的合约地址列表（过滤掉非本网地址）
	var targetAddresses []string
	if tradeTypes, ok := networkTokenMap[e.Network]; ok {
		for addr, mappedType := range contractMap {
			for _, tType := range tradeTypes {
				if mappedType == tType {
					targetAddresses = append(targetAddresses, addr)
				}
			}
		}
	}

	// 如果没有目标地址，则跳过请求，节省流量
	if len(targetAddresses) == 0 {
		return transfers, nil
	}

	// 2. 构造带 address 过滤条件的 eth_getLogs 请求
	addrJson, _ := json.Marshal(targetAddresses)
	payload := fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock":"0x%x","toBlock":"0x%x","address":%s,"topics":["%s"]}],"id":1}`,
		b.From, b.To, string(addrJson), evmTransferEvent)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", e.Endpoint, bytes.NewBuffer([]byte(payload)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Join(errors.New("eth_getLogs 网络请求失败"), err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	data := gjson.ParseBytes(body)
	if data.Get("error").Exists() {
		// 如果节点返回 limit exceeded，此处的 error 会被捕捉到
		return nil, fmt.Errorf("%s eth_getLogs 节点报错: %s", e.Network, data.Get("error").String())
	}

	// 3. 解析返回的日志结果
	for _, itm := range data.Get("result").Array() {
		// 【优化重点】解决大小写问题：
		// 节点返回的 address 可能是全小写，也可能是 Checksum 混合写。
		// 这里不强制转小写，而是使用 Case-Insensitive (不区分大小写) 的方式去 contractMap 匹配。
		rpcAddress := itm.Get("address").String()
		
		var tradeType string
		var matchedContractKey string

		// 遍历 Map 查找匹配的合约（忽略大小写）
		for k, v := range contractMap {
			if strings.EqualFold(k, rpcAddress) {
				tradeType = v
				matchedContractKey = k // 找到配置中原本的 Key（可能是大小写混合的）
				break
			}
		}

		// 如果没找到对应的 TradeType，跳过
		if tradeType == "" {
			continue
		}

		topics := itm.Get("topics").Array()
		if len(topics) < 3 {
			continue
		}

		// 解析转账详情
		from := "0x" + strings.TrimLeft(topics[1].String()[2:], "0")
		recv := "0x" + strings.TrimLeft(topics[2].String()[2:], "0")
		if from == "0x" { from = "0x0" }
		if recv == "0x" { recv = "0x0" }

		amount, ok := big.NewInt(0).SetString(itm.Get("data").String()[2:], 16)
		if !ok || amount.Sign() <= 0 {
			continue
		}

		blockNum, _ := strconv.ParseInt(itm.Get("blockNumber").String(), 0, 64)

		// 注意：decimals 获取时使用 matchedContractKey，确保能取到值
		transfers = append(transfers, transfer{
			Network:     e.Network,
			FromAddress: from,
			RecvAddress: recv,
			Amount:      decimal.NewFromBigInt(amount, decimals[matchedContractKey]), 
			TxHash:      itm.Get("transactionHash").String(),
			BlockNum:    blockNum,
			Timestamp:   timestamp[itm.Get("blockNumber").String()],
			TradeType:   tradeType,
		})
	}

	return transfers, nil
}

// tradeConfirmHandle 确认订单状态逻辑
func (e *evm) tradeConfirmHandle(ctx context.Context) {
	var orders = getConfirmingOrders(networkTokenMap[e.Network])
	var wg sync.WaitGroup

	for _, order := range orders {
		wg.Add(1)
		go func(o model.TradeOrders) {
			defer wg.Done()
			
			post := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["%s"],"id":1}`, o.TradeHash))
			req, err := http.NewRequestWithContext(ctx, "POST", e.Endpoint, bytes.NewBuffer(post))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			data := gjson.ParseBytes(body)
			if data.Get("result.status").String() == "0x1" {
				markFinalConfirmed(o)
			}
		}(order)
	}
	wg.Wait()
}

// rollBreak 检查是否有必要继续扫块 (无待支付订单且无回调地址时跳过)
func rollBreak(network string) bool {
	token, ok := networkTokenMap[network]
	if !ok {
		return true
	}

	var count int64 = 0
	model.DB.Model(&model.TradeOrders{}).Where("status = ? and trade_type in (?)", model.OrderStatusWaiting, token).Count(&count)
	if count > 0 {
		return false
	}

	model.DB.Model(&model.WalletAddress{}).Where("other_notify = ? and trade_type in (?)", model.OtherNotifyEnable, token).Count(&count)
	if count > 0 {
		return false
	}

	return true
}
