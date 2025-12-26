package task

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcutil/base58"
	"github.com/panjf2000/ants/v2"
	"github.com/shopspring/decimal"
	"github.com/tidwall/gjson"
	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/log"
	"github.com/v03413/bepusdt/app/model"
)

type solana struct {
	// 由于改为按地址扫描，原有的 slot 计数器仅作记录参考
	lastProcessedSignature string 
}

type solanaTokenOwner struct {
	TradeType string
	Address   string
}

var sol solana

var solSplToken = map[string]string{
	conf.UsdtSolana: model.OrderTradeTypeUsdtSolana,
	conf.UsdcSolana: model.OrderTradeTypeUsdcSolana,
}

func init() {
	sol = solana{}
	// 核心修改：改为每 30 秒执行一次地址轮询扫描
	register(task{callback: sol.addressScan, duration: time.Second * 30})
	register(task{callback: sol.tradeConfirmHandle, duration: time.Second * 30})
}

// addressScan 核心逻辑：只查数据库里存在的待支付地址
func (s *solana) addressScan(ctx context.Context) {
	var addresses []string
	tokenTypes := []string{model.OrderTradeTypeUsdtSolana, model.OrderTradeTypeUsdcSolana}

	// 获取所有开启了通知或有待支付订单的地址
	model.DB.Model(&model.WalletAddress{}).
		Where("trade_type IN (?) AND other_notify = ?", tokenTypes, model.OtherNotifyEnable).
		Pluck("address", &addresses)

	if len(addresses) == 0 {
		return
	}

	// 并发处理地址扫描
	p, _ := ants.NewPoolWithFunc(5, func(i interface{}) {
		addr := i.(string)
		s.scanSingleAddress(ctx, addr)
	})
	defer p.Release()

	for _, addr := range addresses {
		_ = p.Invoke(addr)
	}
}

func (s *solana) scanSingleAddress(ctx context.Context, address string) {
	// 获取该地址最近 10 条交易签名
	payload := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getSignaturesForAddress","params":["%s",{"limit":10}]}`, address)
	resp, err := client.Post(conf.GetSolanaRpcEndpoint(), "application/json", bytes.NewBuffer([]byte(payload)))
	if err != nil {
		log.Warn("Solana getSignatures Error:", err)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	signatures := gjson.GetBytes(body, "result").Array()

	for _, sigItm := range signatures {
		// 如果有错误则跳过
		if sigItm.Get("err").Exists() && !sigItm.Get("err").IsNull() {
			continue
		}
		
		signature := sigItm.Get("signature").String()
		blockTime := sigItm.Get("blockTime").Int()
		slot := sigItm.Get("slot").Int()

		// 解析单笔交易详情
		s.processTransaction(ctx, signature, slot, blockTime)
	}
}

func (s *solana) processTransaction(ctx context.Context, signature string, slot int64, blockTime int64) {
	// 仅请求单笔交易详情，transactionDetails 为 full 是安全的，因为只有一笔
	payload := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getTransaction","params":["%s",{"encoding":"json","maxSupportedTransactionVersion":0}]}`, signature)
	resp, err := client.Post(conf.GetSolanaRpcEndpoint(), "application/json", bytes.NewBuffer([]byte(payload)))
	if err != nil {
		return
	}
	defer resp.Body.Close()
	
	body, _ := io.ReadAll(resp.Body)
	data := gjson.GetBytes(body, "result")
	
	if data.Get("meta.err").Exists() && !data.Get("meta.err").IsNull() {
		return
	}

	timestamp := time.Unix(blockTime, 0)

	// --- 复用原有的解析逻辑 ---
	accountKeys := make([]string, 0)
	for _, key := range data.Get("transaction.message.accountKeys").Array() {
		accountKeys = append(accountKeys, key.String())
	}
	for _, v := range []string{"readonly", "writable"} {
		for _, key := range data.Get("meta.loadedAddresses." + v).Array() {
			accountKeys = append(accountKeys, key.String())
		}
	}

	splTokenIndex := int64(-1)
	for i, v := range accountKeys {
		if v == conf.SolSplToken {
			splTokenIndex = int64(i)
			break
		}
	}

	if splTokenIndex == -1 {
		return
	}

	tokenAccountMap := make(map[string]solanaTokenOwner)
	for _, v := range []string{"postTokenBalances", "preTokenBalances"} {
		for _, itm := range data.Get("meta." + v).Array() {
			tradeType, ok := solSplToken[itm.Get("mint").String()]
			if !ok || itm.Get("programId").String() != conf.SolSplToken {
				continue
			}
			tokenAccountMap[accountKeys[itm.Get("accountIndex").Int()]] = solanaTokenOwner{
				TradeType: tradeType,
				Address:   itm.Get("owner").String(),
			}
		}
	}

	transArr := make([]transfer, 0)
	// 解析外部指令
	for _, instr := range data.Get("transaction.message.instructions").Array() {
		if instr.Get("programIdIndex").Int() != splTokenIndex {
			continue
		}
		transArr = append(transArr, s.parseTransfer(instr, accountKeys, tokenAccountMap))
	}
	// 解析内部指令
	for _, itm := range data.Get("meta.innerInstructions").Array() {
		for _, instr := range itm.Get("instructions").Array() {
			if instr.Get("programIdIndex").Int() != splTokenIndex {
				continue
			}
			transArr = append(transArr, s.parseTransfer(instr, accountKeys, tokenAccountMap))
		}
	}

	result := make([]transfer, 0)
	for _, t := range transArr {
		if t.FromAddress == "" || t.RecvAddress == "" || t.Amount.IsZero() {
			continue
		}
		t.TxHash = signature
		t.Network = conf.Solana
		t.BlockNum = slot
		t.Timestamp = timestamp
		result = append(result, t)
	}

	if len(result) > 0 {
		transferQueue.In <- result
	}
}

func (s *solana) parseTransfer(instr gjson.Result, accountKeys []string, tokenAccountMap map[string]solanaTokenOwner) transfer {
	accounts := instr.Get("accounts").Array()
	trans := transfer{}
	if len(accounts) < 3 {
		return trans
	}

	data := base58.Decode(instr.Get("data").String())
	dLen := len(data)
	if dLen < 9 {
		return trans
	}

	isTransfer := data[0] == 3 && dLen == 9
	isTransferChecked := data[0] == 12 && dLen == 10
	if !isTransfer && !isTransferChecked {
		return trans
	}

	var exp int32 = -6
	if isTransferChecked {
		exp = int32(data[9]) * -1
	}

	from, ok := tokenAccountMap[accountKeys[accounts[0].Int()]]
	if !ok {
		return trans
	}

	trans.FromAddress = from.Address
	trans.RecvAddress = tokenAccountMap[accountKeys[accounts[1].Int()]].Address
	if isTransferChecked {
		trans.RecvAddress = tokenAccountMap[accountKeys[accounts[2].Int()]].Address
	}

	buf := make([]byte, 8)
	copy(buf[:], data[1:9])
	number := binary.LittleEndian.Uint64(buf)
	b := new(big.Int)
	b.SetUint64(number)
	trans.TradeType = from.TradeType
	trans.Amount = decimal.NewFromBigInt(b, exp)

	return trans
}

func (s *solana) tradeConfirmHandle(ctx context.Context) {
	var orders = getConfirmingOrders(networkTokenMap[conf.Solana])
	var wg sync.WaitGroup

	handle := func(o model.TradeOrders) {
		payload := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getSignatureStatuses","params":[["%s"],{"searchTransactionHistory":true}]}`, o.TradeHash)
		req, _ := http.NewRequestWithContext(ctx, "POST", conf.GetSolanaRpcEndpoint(), bytes.NewBuffer([]byte(payload)))
		resp, err := client.Do(req)
		if err != nil {
			return
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		data := gjson.ParseBytes(body)
		
		status := data.Get("result.value.0.confirmationStatus").String()
		// Solana 建议等待 finalized 状态以确保不可逆
		if status == "finalized" {
			markFinalConfirmed(o)
		}
	}

	for _, order := range orders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handle(order)
		}()
	}
	wg.Wait()
}
