/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

                 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package committer

import (
	"github.com/hyperledger/fabric/core/committer/txvalidator"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/events/producer"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/op/go-logging"
	"github.com/GuardTime/go-ksi"
	"github.com/kelseyhightower/envconfig"
)

//--------!!!IMPORTANT!!-!!IMPORTANT!!-!!IMPORTANT!!---------
// This is used merely to complete the loop for the "skeleton"
// path so we can reason about and  modify committer component
// more effectively using code.

var logger *logging.Logger // package-level logger

func init() {
	logger = logging.MustGetLogger("committer")
}

// LedgerCommitter is the implementation of  Committer interface
// it keeps the reference to the ledger to commit blocks and retreive
// chain information
type LedgerCommitter struct {
	ledger    ledger.PeerLedger
	validator txvalidator.Validator
}

// NewLedgerCommitter is a factory function to create an instance of the committer
func NewLedgerCommitter(ledger ledger.PeerLedger, validator txvalidator.Validator) *LedgerCommitter {
	return &LedgerCommitter{ledger: ledger, validator: validator}
}

func initKSIContext() (*ksi.KSI, error) {
	var s ksi.ClientSettings
    // Using envconfig to load the KSI ClientSettings from the environment
	envconfig.Process("ksi", &s)
	signer, err := ksi.NewKSIFromClientSettings(s)
	if err != nil {
        return nil, err
	}
	err = signer.SetPubFileUrl("http://verify.guardtime.com/ksi-publications.bin","publications@guardtime.com")
	if err != nil {
        return nil, err
	}
	return signer, nil
}

// Commit commits block to into the ledger
// Note, it is important that this always be called serially
func (lc *LedgerCommitter) Commit(block *common.Block) error {
	// Validate and mark invalid transactions
	logger.Debug("Validating block")
	if err := lc.validator.Validate(block); err != nil {
		return err
	}

	if err := lc.ledger.Commit(block); err != nil {
		return err
	}
	logger.Debugf("Successfully committed block %d with hash %x ", block.Header.Number, block.Header.DataHash )
	signer, err := initKSIContext()
	if( err != nil ) {
		logger.Errorf("Error while connecting to the KSI Gateway ",err)
	} else {
		logger.Debugf("Generating KSI Signature ...")
		
		signer.SetLogLevel(ksi.LogLevelInfo)
		sig, err := signer.Sign(block.Header.DataHash)
		if err != nil {
			logger.Errorf("Unable to sign bytes. Error: %v\n", err.Error())
		} else {
			logger.Debugf("Adding the KSI Signature to the Block metadata.")
			signatureBytes, err := sig.Bytes()
			if err != nil {
				logger.Errorf("Failed to serialize the KSI Signature to raw bytes. Error: %v\n", err.Error())
			} else {
				block.Metadata.Metadata = append(block.Metadata.Metadata, signatureBytes);
				logger.Debugf("Now, the metadata array has %d elements",len(block.Metadata.Metadata))
			}
		}
	}
	

	// send block event *after* the block has been committed
	if err := producer.SendProducerBlockEvent(block); err != nil {
		logger.Errorf("Error publishing block %d, because: %v", block.Header.Number, err)
	}

	return nil
}

// LedgerHeight returns recently committed block sequence number
func (lc *LedgerCommitter) LedgerHeight() (uint64, error) {
	var info *common.BlockchainInfo
	var err error
	if info, err = lc.ledger.GetBlockchainInfo(); err != nil {
		logger.Errorf("Cannot get blockchain info, %s\n", info)
		return uint64(0), err
	}

	return info.Height, nil
}

// GetBlocks used to retrieve blocks with sequence numbers provided in the slice
func (lc *LedgerCommitter) GetBlocks(blockSeqs []uint64) []*common.Block {
	var blocks []*common.Block

	for _, seqNum := range blockSeqs {
		if blck, err := lc.ledger.GetBlockByNumber(seqNum); err != nil {
			logger.Errorf("Not able to acquire block num %d, from the ledger skipping...\n", seqNum)
			continue
		} else {
			logger.Debug("Appending next block with seqNum = ", seqNum, " to the resulting set")
			blocks = append(blocks, blck)
		}
	}

	return blocks
}

// Close the ledger
func (lc *LedgerCommitter) Close() {
	lc.ledger.Close()
}
