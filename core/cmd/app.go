package cmd

import (
	"fmt"
	"os"
	"regexp"

	"github.com/urfave/cli"

	"github.com/smartcontractkit/chainlink/core/static"
)

func removeHidden(cmds ...cli.Command) []cli.Command {
	var ret []cli.Command
	for _, cmd := range cmds {
		if cmd.Hidden {
			continue
		}
		ret = append(ret, cmd)
	}
	return ret
}

// NewApp returns the command-line parser/function-router for the given client
func NewApp(client *Client) *cli.App {
	app := cli.NewApp()
	app.Usage = "CLI for Chainlink"
	app.Version = fmt.Sprintf("%v@%v", static.Version, static.Sha)
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "json, j",
			Usage: "json output as opposed to table",
		},
	}
	app.Before = func(c *cli.Context) error {
		if c.Bool("json") {
			client.Renderer = RendererJSON{Writer: os.Stdout}
		}
		return nil
	}
	app.Commands = removeHidden([]cli.Command{
		{
			Name:  "admin",
			Usage: "Commands for remotely taking admin related actions",
			Subcommands: []cli.Command{
				{
					Name:   "chpass",
					Usage:  "Change your API password remotely",
					Action: client.ChangePassword,
				},
				{
					Name:   "login",
					Usage:  "Login to remote client by creating a session cookie",
					Action: client.RemoteLogin,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "file, f",
							Usage: "text file holding the API email and password needed to create a session cookie",
						},
					},
				},
			},
		},

		{
			Name:    "attempts",
			Aliases: []string{"txas"},
			Usage:   "Commands for managing Ethereum Transaction Attempts",
			Subcommands: []cli.Command{
				{
					Name:   "list",
					Usage:  "List the Transaction Attempts in descending order",
					Action: client.IndexTxAttempts,
					Flags: []cli.Flag{
						cli.IntFlag{
							Name:  "page",
							Usage: "page of results to display",
						},
					},
				},
			},
		},

		{
			Name:    "blocks",
			Aliases: []string{},
			Usage:   "Commands for managing blocks",
			Subcommands: []cli.Command{
				{
					Name:   "replay",
					Usage:  "Replays block data from the given number",
					Action: client.ReplayFromBlock,
					Flags: []cli.Flag{
						cli.IntFlag{
							Name:  "block-number",
							Usage: "Block number to replay from",
						},
					},
				},
			},
		},

		{
			Name:  "bridges",
			Usage: "Commands for Bridges communicating with External Adapters",
			Subcommands: []cli.Command{
				{
					Name:   "create",
					Usage:  "Create a new Bridge to an External Adapter",
					Action: client.CreateBridge,
				},
				{
					Name:   "destroy",
					Usage:  "Destroys the Bridge for an External Adapter",
					Action: client.RemoveBridge,
				},
				{
					Name:   "list",
					Usage:  "List all Bridges to External Adapters",
					Action: client.IndexBridges,
					Flags: []cli.Flag{
						cli.IntFlag{
							Name:  "page",
							Usage: "page of results to display",
						},
					},
				},
				{
					Name:   "show",
					Usage:  "Show a Bridge's details",
					Action: client.ShowBridge,
				},
			},
		},

		{
			Name:  "config",
			Usage: "Commands for the node's configuration",
			Subcommands: []cli.Command{
				{
					Name:   "list",
					Usage:  "Show the node's environment variables",
					Action: client.GetConfiguration,
				},
				{
					Name:   "setgasprice",
					Usage:  "Set the default gas price to use for outgoing transactions",
					Action: client.SetEvmGasPriceDefault,
					Flags: []cli.Flag{
						cli.BoolFlag{
							Name:  "gwei",
							Usage: "Specify amount in gwei",
						},
						cli.StringFlag{
							Name:  "evmChainID",
							Usage: "(optional) specify the chain ID for which to make the update",
						},
					},
				},
				{
					Name:   "loglevel",
					Usage:  "Set log level",
					Action: client.SetLogLevel,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "level",
							Usage: "set log level for node (debug||info||warn||error)",
						},
					},
				},
				{
					Name:   "logpkg",
					Usage:  "Set package specific logging",
					Action: client.SetLogPkg,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "pkg",
							Usage: "set log filter for package specific logging",
						},
						cli.StringFlag{
							Name:  "level",
							Usage: "set log level for specified pkg",
						},
					},
				},
				{
					Name:   "logsql",
					Usage:  "Enable/disable sql statement logging",
					Action: client.SetLogSQL,
					Flags: []cli.Flag{
						cli.BoolFlag{
							Name:  "enable",
							Usage: "enable sql logging",
						},
						cli.BoolFlag{
							Name:  "disable",
							Usage: "disable sql logging",
						},
					},
				},
			},
		},

		{
			Name:  "jobs",
			Usage: "Commands for managing Jobs",
			Subcommands: []cli.Command{
				{
					Name:   "list",
					Usage:  "List all jobs",
					Action: client.ListJobs,
					Flags: []cli.Flag{
						cli.IntFlag{
							Name:  "page",
							Usage: "page of results to display",
						},
					},
				},
				{
					Name:   "show",
					Usage:  "Show a job",
					Action: client.ShowJob,
				},
				{
					Name:   "create",
					Usage:  "Create a job",
					Action: client.CreateJob,
				},
				{
					Name:   "delete",
					Usage:  "Delete a job",
					Action: client.DeleteJob,
				},
				{
					Name:   "run",
					Usage:  "Trigger a job run",
					Action: client.TriggerPipelineRun,
				},
			},
		},
		{
			Name:  "keys",
			Usage: "Commands for managing various types of keys used by the Chainlink node",
			Subcommands: []cli.Command{
				{
					Name:  "eth",
					Usage: "Remote commands for administering the node's Ethereum keys",
					Subcommands: cli.Commands{
						{
							Name:   "create",
							Usage:  "Create a key in the node's keystore alongside the existing key; to create an original key, just run the node",
							Action: client.CreateETHKey,
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "evmChainID",
									Usage: "Chain ID for the key. If left blank, default chain will be used.",
								},
								cli.Uint64Flag{
									Name:  "maxGasPriceGWei",
									Usage: "Optional maximum gas price (GWei) for the creating key.",
								},
							},
						},
						{
							Name:   "update",
							Usage:  "Update the existing key's parameters",
							Action: client.UpdateETHKey,
							Flags: []cli.Flag{
								cli.Uint64Flag{
									Name:  "maxGasPriceGWei",
									Usage: "Maximum gas price (GWei) for the specified key.",
								},
							},
						},
						{
							Name:   "list",
							Usage:  "List available Ethereum accounts with their ETH & LINK balances, nonces, and other metadata",
							Action: client.ListETHKeys,
						},
						{
							Name:  "delete",
							Usage: format(`Delete the ETH key by address`),
							Flags: []cli.Flag{
								cli.BoolFlag{
									Name:  "yes, y",
									Usage: "skip the confirmation prompt",
								},
								cli.BoolFlag{
									Name:  "hard",
									Usage: "hard-delete the key instead of archiving (irreversible!)",
								},
							},
							Action: client.DeleteETHKey,
						},
						{
							Name:  "import",
							Usage: format(`Import an ETH key from a JSON file`),
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "oldpassword, p",
									Usage: "`FILE` containing the password used to encrypt the key in the JSON file",
								},
								cli.StringFlag{
									Name:  "evmChainID",
									Usage: "Chain ID for the key. If left blank, default chain will be used.",
								},
							},
							Action: client.ImportETHKey,
						},
						{
							Name:  "export",
							Usage: format(`Exports an ETH key to a JSON file`),
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "newpassword, p",
									Usage: "`FILE` containing the password to encrypt the key (required)",
								},
								cli.StringFlag{
									Name:  "output, o",
									Usage: "Path where the JSON file will be saved (required)",
								},
							},
							Action: client.ExportETHKey,
						},
					},
				},

				{
					Name:  "p2p",
					Usage: "Remote commands for administering the node's p2p keys",
					Subcommands: cli.Commands{
						{
							Name:   "create",
							Usage:  format(`Create a p2p key, encrypted with password from the password file, and store it in the database.`),
							Action: client.CreateP2PKey,
						},
						{
							Name:  "delete",
							Usage: format(`Delete the encrypted P2P key by id`),
							Flags: []cli.Flag{
								cli.BoolFlag{
									Name:  "yes, y",
									Usage: "skip the confirmation prompt",
								},
								cli.BoolFlag{
									Name:  "hard",
									Usage: "hard-delete the key instead of archiving (irreversible!)",
								},
							},
							Action: client.DeleteP2PKey,
						},
						{
							Name:   "list",
							Usage:  format(`List available P2P keys`),
							Action: client.ListP2PKeys,
						},
						{
							Name:  "import",
							Usage: format(`Imports a P2P key from a JSON file`),
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "oldpassword, p",
									Usage: "`FILE` containing the password used to encrypt the key in the JSON file",
								},
							},
							Action: client.ImportP2PKey,
						},
						{
							Name:  "export",
							Usage: format(`Exports a P2P key to a JSON file`),
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "newpassword, p",
									Usage: "`FILE` containing the password to encrypt the key (required)",
								},
								cli.StringFlag{
									Name:  "output, o",
									Usage: "`FILE` where the JSON file will be saved (required)",
								},
							},
							Action: client.ExportP2PKey,
						},
					},
				},

				{
					Name:  "csa",
					Usage: "Remote commands for administering the node's CSA keys",
					Subcommands: cli.Commands{
						{
							Name:   "create",
							Usage:  format(`Create a CSA key, encrypted with password from the password file, and store it in the database.`),
							Action: client.CreateCSAKey,
						},
						{
							Name:   "list",
							Usage:  format(`List available CSA keys`),
							Action: client.ListCSAKeys,
						},
						{
							Name:  "import",
							Usage: format(`Imports a CSA key from a JSON file.`),
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "oldpassword, p",
									Usage: "`FILE` containing the password used to encrypt the key in the JSON file",
								},
							},
							Action: client.ImportCSAKey,
						},
						{
							Name:  "export",
							Usage: format(`Exports an existing CSA key by its ID.`),
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "newpassword, p",
									Usage: "`FILE` containing the password to encrypt the key (required)",
								},
								cli.StringFlag{
									Name:  "output, o",
									Usage: "`FILE` where the JSON file will be saved (required)",
								},
							},
							Action: client.ExportCSAKey,
						},
					},
				},

				{
					Name:  "ocr",
					Usage: "Remote commands for administering the node's legacy off chain reporting keys",
					Subcommands: cli.Commands{
						{
							Name:   "create",
							Usage:  format(`Create an OCR key bundle, encrypted with password from the password file, and store it in the database`),
							Action: client.CreateOCRKeyBundle,
						},
						{
							Name:  "delete",
							Usage: format(`Deletes the encrypted OCR key bundle matching the given ID`),
							Flags: []cli.Flag{
								cli.BoolFlag{
									Name:  "yes, y",
									Usage: "skip the confirmation prompt",
								},
								cli.BoolFlag{
									Name:  "hard",
									Usage: "hard-delete the key instead of archiving (irreversible!)",
								},
							},
							Action: client.DeleteOCRKeyBundle,
						},
						{
							Name:   "list",
							Usage:  format(`List available OCR key bundles`),
							Action: client.ListOCRKeyBundles,
						},
						{
							Name:  "import",
							Usage: format(`Imports an OCR key bundle from a JSON file`),
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "oldpassword, p",
									Usage: "`FILE` containing the password used to encrypt the key in the JSON file",
								},
							},
							Action: client.ImportOCRKey,
						},
						{
							Name:  "export",
							Usage: format(`Exports an OCR key bundle to a JSON file`),
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "newpassword, p",
									Usage: "`FILE` containing the password to encrypt the key (required)",
								},
								cli.StringFlag{
									Name:  "output, o",
									Usage: "`FILE` where the JSON file will be saved (required)",
								},
							},
							Action: client.ExportOCRKey,
						},
					},
				},

				{
					Name:  "ocr2",
					Usage: "Remote commands for administering the node's off chain reporting keys",
					Subcommands: cli.Commands{
						{
							Name:   "create",
							Usage:  format(`Create an OCR2 key bundle, encrypted with password from the password file, and store it in the database`),
							Action: client.CreateOCR2KeyBundle,
						},
						{
							Name:  "delete",
							Usage: format(`Deletes the encrypted OCR2 key bundle matching the given ID`),
							Flags: []cli.Flag{
								cli.BoolFlag{
									Name:  "yes, y",
									Usage: "skip the confirmation prompt",
								},
								cli.BoolFlag{
									Name:  "hard",
									Usage: "hard-delete the key instead of archiving (irreversible!)",
								},
							},
							Action: client.DeleteOCR2KeyBundle,
						},
						{
							Name:   "list",
							Usage:  format(`List available OCR2 key bundles`),
							Action: client.ListOCR2KeyBundles,
						},
						{
							Name:  "import",
							Usage: format(`Imports an OCR2 key bundle from a JSON file`),
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "oldpassword, p",
									Usage: "`FILE` containing the password used to encrypt the key in the JSON file",
								},
							},
							Action: client.ImportOCR2Key,
						},
						{
							Name:  "export",
							Usage: format(`Exports an OCR2 key bundle to a JSON file`),
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "newpassword, p",
									Usage: "`FILE` containing the password to encrypt the key (required)",
								},
								cli.StringFlag{
									Name:  "output, o",
									Usage: "`FILE` where the JSON file will be saved (required)",
								},
							},
							Action: client.ExportOCR2Key,
						},
					},
				},

				{
					Name:  "solana",
					Usage: "Remote commands for administering the node's solana keys",
					Subcommands: cli.Commands{
						{
							Name:   "create",
							Usage:  "Create a Solana key",
							Action: client.CreateSolanaKey,
						},
						{
							Name:  "import",
							Usage: "Import Solana key from keyfile",
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "oldpassword, p",
									Usage: "`FILE` containing the password used to encrypt the key in the JSON file",
								},
							},
							Action: client.ImportSolanaKey,
						},
						{
							Name:  "export",
							Usage: "Export Solana key to keyfile",
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "newpassword, p",
									Usage: "`FILE` containing the password to encrypt the key (required)",
								},
								cli.StringFlag{
									Name:  "output, o",
									Usage: "`FILE` where the JSON file will be saved (required)",
								},
							},
							Action: client.ExportSolanaKey,
						},
						{
							Name:  "delete",
							Usage: "Delete Solana key if present",
							Flags: []cli.Flag{
								cli.BoolFlag{
									Name:  "yes, y",
									Usage: "skip the confirmation prompt",
								},
								cli.BoolFlag{
									Name:  "hard",
									Usage: "hard-delete the key instead of archiving (irreversible!)",
								},
							},
							Action: client.DeleteSolanaKey,
						},
						{
							Name: "list", Usage: "List the Solana keys",
							Action: client.ListSolanaKeys,
						},
					},
				},

				{
					Name:  "terra",
					Usage: "Remote commands for administering the node's terra keys",
					Subcommands: cli.Commands{
						{
							Name:   "create",
							Usage:  "Create a Terra key",
							Action: client.CreateTerraKey,
						},
						{
							Name:  "import",
							Usage: "Import Terra key from keyfile",
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "oldpassword, p",
									Usage: "`FILE` containing the password used to encrypt the key in the JSON file",
								},
							},
							Action: client.ImportTerraKey,
						},
						{
							Name:  "export",
							Usage: "Export Terra key to keyfile",
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "newpassword, p",
									Usage: "`FILE` containing the password to encrypt the key (required)",
								},
								cli.StringFlag{
									Name:  "output, o",
									Usage: "`FILE` where the JSON file will be saved (required)",
								},
							},
							Action: client.ExportTerraKey,
						},
						{
							Name:  "delete",
							Usage: "Delete Terra key if present",
							Flags: []cli.Flag{
								cli.BoolFlag{
									Name:  "yes, y",
									Usage: "skip the confirmation prompt",
								},
								cli.BoolFlag{
									Name:  "hard",
									Usage: "hard-delete the key instead of archiving (irreversible!)",
								},
							},
							Action: client.DeleteTerraKey,
						},
						{
							Name: "list", Usage: "List the Terra keys",
							Action: client.ListTerraKeys,
						},
					},
				},

				{
					Name:  "vrf",
					Usage: "Remote commands for administering the node's vrf keys",
					Subcommands: cli.Commands{
						{
							Name:   "create",
							Usage:  "Create a VRF key",
							Action: client.CreateVRFKey,
						},
						{
							Name:  "import",
							Usage: "Import VRF key from keyfile",
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "oldpassword, p",
									Usage: "`FILE` containing the password used to encrypt the key in the JSON file",
								},
							},
							Action: client.ImportVRFKey,
						},
						{
							Name:  "export",
							Usage: "Export VRF key to keyfile",
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "newpassword, p",
									Usage: "`FILE` containing the password to encrypt the key (required)",
								},
								cli.StringFlag{
									Name:  "output, o",
									Usage: "`FILE` where the JSON file will be saved (required)",
								},
							},
							Action: client.ExportVRFKey,
						},
						{
							Name: "delete",
							Usage: "Archive or delete VRF key from memory and the database, if present. " +
								"Note that jobs referencing the removed key will also be removed.",
							Flags: []cli.Flag{
								cli.StringFlag{Name: "publicKey, pk"},
								cli.BoolFlag{
									Name:  "yes, y",
									Usage: "skip the confirmation prompt",
								},
								cli.BoolFlag{
									Name:  "hard",
									Usage: "hard-delete the key instead of archiving (irreversible!)",
								},
							},
							Action: client.DeleteVRFKey,
						},
						{
							Name: "list", Usage: "List the VRF keys",
							Action: client.ListVRFKeys,
						},
					},
				},
			},
		},
		{
			Name:        "node",
			Aliases:     []string{"local"},
			Usage:       "Commands for admin actions that must be run locally",
			Description: "Commands can only be run from on the same machine as the Chainlink node.",
			Subcommands: []cli.Command{
				{
					Name:        "deleteuser",
					Usage:       "Erase the *local node's* user and corresponding session to force recreation on next node launch.",
					Description: "Does not work remotely over API.",
					Action:      client.DeleteUser,
				},
				{
					Name:   "setnextnonce",
					Usage:  "Manually set the next nonce for a key. This should NEVER be necessary during normal operation. USE WITH CAUTION: Setting this incorrectly can break your node.",
					Action: client.SetNextNonce,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "address",
							Usage: "address of the key for which to set the nonce",
						},
						cli.Uint64Flag{
							Name:  "nextNonce",
							Usage: "the next nonce in the sequence",
						},
					},
				},
				{
					Name:    "start",
					Aliases: []string{"node", "n"},
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "api, a",
							Usage: "text file holding the API email and password, each on a line",
						},
						cli.BoolFlag{
							Name:  "debug, d",
							Usage: "set logger level to debug",
						},
						cli.StringFlag{
							Name:  "password, p",
							Usage: "text file holding the password for the node's account",
						},
						cli.StringFlag{
							Name:  "vrfpassword, vp",
							Usage: "text file holding the password for the vrf keys; enables Chainlink VRF oracle",
						},
					},
					Usage:  "Run the Chainlink node",
					Action: client.RunNode,
				},
				{
					Name:   "rebroadcast-transactions",
					Usage:  "Manually rebroadcast txs matching nonce range with the specified gas price. This is useful in emergencies e.g. high gas prices and/or network congestion to forcibly clear out the pending TX queue",
					Action: client.RebroadcastTransactions,
					Flags: []cli.Flag{
						cli.Uint64Flag{
							Name:  "beginningNonce, b",
							Usage: "beginning of nonce range to rebroadcast",
						},
						cli.Uint64Flag{
							Name:  "endingNonce, e",
							Usage: "end of nonce range to rebroadcast (inclusive)",
						},
						cli.Uint64Flag{
							Name:  "gasPriceWei, g",
							Usage: "gas price (in Wei) to rebroadcast transactions at",
						},
						cli.StringFlag{
							Name:  "password, p",
							Usage: "text file holding the password for the node's account",
						},
						cli.StringFlag{
							Name:  "address, a",
							Usage: "The address (in hex format) for the key which we want to rebroadcast transactions",
						},
						cli.StringFlag{
							Name:  "evmChainID",
							Usage: "Chain ID for which to rebroadcast transactions. If left blank, ETH_CHAIN_ID will be used.",
						},
						cli.Uint64Flag{
							Name:  "gasLimit",
							Usage: "OPTIONAL: gas limit to use for each transaction ",
						},
					},
				},
				{
					Name:   "status",
					Usage:  "Displays the health of various services running inside the node.",
					Action: client.Status,
					Flags:  []cli.Flag{},
				},
				{
					Name:        "db",
					Usage:       "Commands for managing the database.",
					Description: "Potentially destructive commands for managing the database.",
					Subcommands: []cli.Command{
						{
							Name:   "reset",
							Usage:  "Drop, create and migrate database. Useful for setting up the database in order to run tests or resetting the dev database. WARNING: This will ERASE ALL DATA for the specified DATABASE_URL.",
							Hidden: !client.Config.Dev(),
							Action: client.ResetDatabase,
							Flags: []cli.Flag{
								cli.BoolFlag{
									Name:  "dangerWillRobinson",
									Usage: "set to true to enable dropping non-test databases",
								},
							},
						},
						{
							Name:   "preparetest",
							Usage:  "Reset database and load fixtures.",
							Hidden: !client.Config.Dev(),
							Action: client.PrepareTestDatabase,
							Flags: []cli.Flag{
								cli.BoolFlag{
									Name:  "user-only",
									Usage: "only include test user fixture",
								},
							},
						},
						{
							Name:   "version",
							Usage:  "Display the current database version.",
							Action: client.VersionDatabase,
							Flags:  []cli.Flag{},
						},
						{
							Name:   "status",
							Usage:  "Display the current database migration status.",
							Action: client.StatusDatabase,
							Flags:  []cli.Flag{},
						},
						{
							Name:   "migrate",
							Usage:  "Migrate the database to the latest version.",
							Action: client.MigrateDatabase,
							Flags:  []cli.Flag{},
						},
						{
							Name:   "rollback",
							Usage:  "Roll back the database to a previous <version>. Rolls back a single migration if no version specified.",
							Action: client.RollbackDatabase,
							Flags:  []cli.Flag{},
						},
						{
							Name:   "create-migration",
							Usage:  "Create a new migration.",
							Hidden: !client.Config.Dev(),
							Action: client.CreateMigration,
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "type",
									Usage: "set to `go` to generate a .go migration (instead of .sql)",
								},
							},
						},
					},
				},
			},
		},

		{
			Name:   "initiators",
			Usage:  "Commands for managing External Initiators",
			Hidden: !client.Config.Dev() && !client.Config.FeatureExternalInitiators(),
			Subcommands: []cli.Command{
				{
					Name:   "create",
					Usage:  "Create an authentication key for a user of External Initiators",
					Action: client.CreateExternalInitiator,
				},
				{
					Name:   "destroy",
					Usage:  "Remove an external initiator by name",
					Action: client.DeleteExternalInitiator,
				},
				{
					Name:   "list",
					Usage:  "List all external initiators",
					Action: client.IndexExternalInitiators,
				},
			},
		},

		{
			Name:  "txs",
			Usage: "Commands for handling Ethereum transactions",
			Subcommands: []cli.Command{
				{
					Name:   "create",
					Usage:  "Send <amount> Eth from node ETH account <fromAddress> to destination <toAddress>.",
					Action: client.SendEther,
				},
				{
					Name:   "list",
					Usage:  "List the Ethereum Transactions in descending order",
					Action: client.IndexTransactions,
					Flags: []cli.Flag{
						cli.IntFlag{
							Name:  "page",
							Usage: "page of results to display",
						},
					},
				},
				{
					Name:   "show",
					Usage:  "get information on a specific Ethereum Transaction",
					Action: client.ShowTransaction,
				},
			},
		},
		{
			Name:  "chains",
			Usage: "Commands for handling chain configuration",
			Subcommands: cli.Commands{
				{
					Name:  "evm",
					Usage: "Commands for handling EVM chains",
					Subcommands: cli.Commands{
						{
							Name:   "create",
							Usage:  "Create a new EVM chain",
							Action: client.CreateChain,
							Flags: []cli.Flag{
								cli.Int64Flag{
									Name:  "id",
									Usage: "chain ID",
								},
							},
						},
						{
							Name:   "delete",
							Usage:  "Delete an EVM chain",
							Action: client.RemoveChain,
						},
						{
							Name:   "list",
							Usage:  "List all chains",
							Action: client.IndexChains,
						},
						{
							Name:   "configure",
							Usage:  "Configure an EVM chain",
							Action: client.ConfigureChain,
							Flags: []cli.Flag{
								cli.Int64Flag{
									Name:  "id",
									Usage: "chain ID",
								},
							},
						},
					},
				},
			},
		},
		{
			Name:  "nodes",
			Usage: "Commands for handling node configuration",
			Subcommands: cli.Commands{
				{
					Name:   "create",
					Usage:  "Create a new node",
					Action: client.CreateNode,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "name",
							Usage: "node name",
						},
						cli.StringFlag{
							Name:  "ws-url",
							Usage: "Websocket URL",
						},
						cli.StringFlag{
							Name:  "http-url",
							Usage: "HTTP URL, optional",
						},
						cli.Int64Flag{
							Name:  "chain-id",
							Usage: "chain ID",
						},
						cli.StringFlag{
							Name:  "type",
							Usage: "primary|secondary",
						},
					},
				},
				{
					Name:   "delete",
					Usage:  "Delete a node",
					Action: client.RemoveNode,
				},
				{
					Name:   "list",
					Usage:  "List all nodes",
					Action: client.IndexNodes,
				},
			},
		},
	}...)
	return app
}

var whitespace = regexp.MustCompile(`\s+`)

// format returns result of replacing all whitespace in s with a single space
func format(s string) string {
	return string(whitespace.ReplaceAll([]byte(s), []byte(" ")))
}
