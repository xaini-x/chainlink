package relay_test

import (
	"testing"

	"github.com/pelletier/go-toml"
	uuid "github.com/satori/go.uuid"
	chainsMock "github.com/smartcontractkit/chainlink/core/chains/evm/mocks"
	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/services/job"
	"github.com/smartcontractkit/chainlink/core/services/keystore/keys/solkey"
	keystoreMock "github.com/smartcontractkit/chainlink/core/services/keystore/mocks"
	"github.com/smartcontractkit/chainlink/core/services/relay"
	"github.com/smartcontractkit/chainlink/core/testdata/testspecs"
	"github.com/smartcontractkit/sqlx"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func makeOCR2JobSpecFromToml(t *testing.T, jobSpecToml string) job.OffchainReporting2OracleSpec {
	t.Helper()

	var ocr2spec job.OffchainReporting2OracleSpec
	err := toml.Unmarshal([]byte(jobSpecToml), &ocr2spec)
	require.NoError(t, err)

	return ocr2spec
}

func TestNewOCR2Provider(t *testing.T) {
	// setup keystore mock
	solKey := new(keystoreMock.Solana)
	solKey.On("Get", mock.AnythingOfType("string")).Return(solkey.Key{}, nil)

	// setup solana key mock
	keystore := new(keystoreMock.Master)
	keystore.On("Solana").Return(solKey, nil)

	d := relay.NewDelegate(&sqlx.DB{}, keystore, &chainsMock.ChainSet{}, logger.NewLogger())

	// struct for testing multiple specs
	specs := []struct {
		name string
		spec string
	}{
		{"solana", testspecs.OCR2SolanaSpecMinimal},
	}

	for _, s := range specs {
		t.Run(s.name, func(t *testing.T) {
			spec := makeOCR2JobSpecFromToml(t, s.spec)

			_, err := d.NewOCR2Provider(uuid.UUID{}, &spec)
			require.NoError(t, err)
		})
	}
}
