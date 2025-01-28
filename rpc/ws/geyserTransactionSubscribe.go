package ws

import (
	"context"
	"fmt"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

type GeyserTransactionResult struct {
	Transaction struct {
		Transaction solana.Data         `json:"transaction"`
		Meta        rpc.TransactionMeta `json:"meta"`
	} `json:"transaction"`
	Signature string `json:"signature"`
	Slot      uint64 `json:"slot"`
}

type GeyserTransactionSubscribeFilter struct {
	Vote            bool     `json:"vote"`
	Failed          bool     `json:"failed"`
	Signature       string   `json:"signature"`
	AccountInclude  []string `json:"accountInclude"`
	AccountExclude  []string `json:"accountExclude"`
	AccountRequired []string `json:"accountRequired"`
}

func NewTransactionSubscribeFilter() *GeyserTransactionSubscribeFilter {
	return &GeyserTransactionSubscribeFilter{}
}

func (f *GeyserTransactionSubscribeFilter) SetVote(vote bool) *GeyserTransactionSubscribeFilter {
	f.Vote = vote
	return f
}

func (f *GeyserTransactionSubscribeFilter) SetFailed(failed bool) *GeyserTransactionSubscribeFilter {
	f.Failed = failed
	return f
}

func (f *GeyserTransactionSubscribeFilter) SetSignature(signature string) *GeyserTransactionSubscribeFilter {
	f.Signature = signature
	return f
}

func (f *GeyserTransactionSubscribeFilter) SetAccountInclude(accountInclude []string) *GeyserTransactionSubscribeFilter {
	f.AccountInclude = accountInclude
	return f
}

func (f *GeyserTransactionSubscribeFilter) SetAccountExclude(accountExclude []string) *GeyserTransactionSubscribeFilter {
	f.AccountExclude = accountExclude
	return f
}

func (f *GeyserTransactionSubscribeFilter) SetAccountRequired(accountRequired []string) *GeyserTransactionSubscribeFilter {
	f.AccountRequired = accountRequired
	return f
}

type GeyserTransactionSubscribeOpts struct {
	Commitment                     rpc.CommitmentType
	Encoding                       solana.EncodingType `json:"encoding,omitempty"`
	TransactionDetails             rpc.TransactionDetailsType
	Rewards                        *bool `json:"showRewards,omitempty"`
	MaxSupportedTransactionVersion *uint64
}

func (cl *Client) GeyserTransactionSubscribe(
	commitment rpc.CommitmentType,
	signature string,
	accountsInclude []string,
	accountsExclude []string,
	accountsRequired []string,
) (*GeyserTransactionSubscription, error) {

	filter := GeyserTransactionSubscribeFilter{
		Vote:            false,
		Failed:          false,
		Signature:       signature,
		AccountInclude:  accountsInclude,
		AccountExclude:  accountsExclude,
		AccountRequired: accountsRequired,
	}
	var rewards = true
	maxSupportedTransaction := uint64(0)

	opts := GeyserTransactionSubscribeOpts{
		Commitment:                     commitment,
		Encoding:                       solana.EncodingBase64,
		TransactionDetails:             rpc.TransactionDetailsFull,
		Rewards:                        &rewards,
		MaxSupportedTransactionVersion: &maxSupportedTransaction,
	}

	return cl.GeyserTransactionSubscribeOpts(filter, &opts)
}

func (cl *Client) GeyserTransactionSubscribeOpts(
	filter GeyserTransactionSubscribeFilter,
	opts *GeyserTransactionSubscribeOpts,
) (*GeyserTransactionSubscription, error) {
	var params []interface{}

	// Construct the filter object dynamically
	filterMap := make(rpc.M)
	if filter.Vote {
		filterMap["vote"] = filter.Vote
	}
	if filter.Failed {
		filterMap["failed"] = filter.Failed
	}
	if filter.Signature != "" {
		filterMap["signature"] = filter.Signature
	}
	if len(filter.AccountInclude) > 0 {
		filterMap["accountInclude"] = filter.AccountInclude
	}
	if len(filter.AccountExclude) > 0 {
		filterMap["accountExclude"] = filter.AccountExclude
	}
	if len(filter.AccountRequired) > 0 {
		filterMap["accountRequired"] = filter.AccountRequired
	}
	params = append(params, filterMap)

	// Construct the options object
	optsMap := make(rpc.M)
	if opts.Commitment != "" {
		optsMap["commitment"] = opts.Commitment
	}
	if opts.TransactionDetails != "" {
		optsMap["transactionDetails"] = opts.TransactionDetails
	}
	if opts.Encoding != "" {
		if !solana.IsAnyOfEncodingType(opts.Encoding,
			solana.EncodingBase64, solana.EncodingBase58, solana.EncodingBase64Zstd) {
			return nil, fmt.Errorf("provided encoding is not supported: %s", opts.Encoding)
		}
		optsMap["encoding"] = opts.Encoding
	}
	if opts.Rewards != nil {
		optsMap["showRewards"] = *opts.Rewards
	}
	if opts.MaxSupportedTransactionVersion != nil {
		optsMap["maxSupportedTransactionVersion"] = *opts.MaxSupportedTransactionVersion
	}

	params = append(params, optsMap)

	genSub, err := cl.subscribe(
		params,
		nil,
		"transactionSubscribe",
		"transactionUnsubscribe",
		func(msg []byte) (interface{}, error) {
			var res GeyserTransactionResult
			err := decodeResponseFromMessage(msg, &res)
			return &res, err
		},
	)
	if err != nil {
		return nil, err
	}
	return &GeyserTransactionSubscription{
		sub: genSub,
	}, nil
}

type GeyserTransactionSubscription struct {
	sub *Subscription
}

func (ts *GeyserTransactionSubscription) Recv(ctx context.Context) (*GeyserTransactionResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case d, ok := <-ts.sub.stream:
		if !ok {
			return nil, ErrSubscriptionClosed
		}
		return d.(*GeyserTransactionResult), nil
	case err := <-ts.sub.err:
		return nil, err
	}
}

func (ts *GeyserTransactionSubscription) Err() <-chan error {
	return ts.sub.err
}

func (ts *GeyserTransactionSubscription) Response() <-chan *GeyserTransactionResult {
	typedChan := make(chan *GeyserTransactionResult, 1)
	go func(ch chan *GeyserTransactionResult) {
		d, ok := <-ts.sub.stream
		if !ok {
			return
		}
		ch <- d.(*GeyserTransactionResult)
	}(typedChan)
	return typedChan
}

func (ts *GeyserTransactionSubscription) Unsubscribe() {
	ts.sub.Unsubscribe()
}
