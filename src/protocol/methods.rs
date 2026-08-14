#[repr(usize)]
pub enum RpcMethod {
    Unknown = 0,
    StakedConnections = 1,
    BatchIdentityLookup = 2,
    CreateWebhook = 3,
    DeleteWebhook = 4,
    GetAccountInfo = 5,
    GetAsset = 6,
    GetAssetBatch = 7,
    GetAssetProof = 8,
    GetAssetProofBatch = 9,
    GetAssetProofs = 10,
    GetAssetSignatures = 11,
    GetAssets = 12,
    GetAssetsByAuthority = 13,
    GetAssetsByCreator = 14,
    GetAssetsByGroup = 15,
    GetAssetsByOwner = 16,
    GetBalance = 17,
    GetBlock = 18,
    GetBlockCommitment = 19,
    GetBlockHeight = 20,
    GetBlockProduction = 21,
    GetBlockTime = 22,
    GetBlocks = 23,
    GetBlocksWithLimit = 24,
    GetBundleStatuses = 25,
    GetClusterNodes = 26,
    GetCompressedAccount = 27,
    GetCompressedAccountProof = 28,
    GetCompressedAccountsByOwner = 29,
    GetCompressedBalance = 30,
    GetCompressedBalanceByOwner = 31,
    GetCompressedMintTokenHolders = 32,
    GetCompressedTokenAccountBalance = 33,
    GetCompressedTokenAccountsByDelegate = 34,
    GetCompressedTokenAccountsByOwner = 35,
    GetCompressedTokenBalancesByOwner = 36,
    GetCompressedTokenBalancesByOwnerV2 = 37,
    GetCompressionSignaturesForAccount = 38,
    GetCompressionSignaturesForAddress = 39,
    GetCompressionSignaturesForOwner = 40,
    GetCompressionSignaturesForTokenOwner = 41,
    GetEpochInfo = 42,
    GetEpochSchedule = 43,
    GetFeeForMessage = 44,
    GetFirstAvailableBlock = 45,
    GetGenesisHash = 46,
    GetHealth = 47,
    GetHighestSnapshotSlot = 48,
    GetIdentity = 49,
    GetIndexerHealth = 50,
    GetIndexerSlot = 51,
    GetInflationGovernor = 52,
    GetInflationRate = 53,
    GetInflationReward = 54,
    GetInflightBundleStatuses = 55,
    GetLargestAccounts = 56,
    GetLatestBlockhash = 57,
    GetLatestCompressionSignatures = 58,
    GetLatestNonVotingSignatures = 59,
    GetLeaderSchedule = 60,
    GetMaxRetransmitSlot = 61,
    GetMaxShredInsertSlot = 62,
    GetMinimumBalanceForRentExemption = 63,
    GetMultipleAccounts = 64,
    GetMultipleCompressedAccountProofs = 65,
    GetMultipleCompressedAccounts = 66,
    GetMultipleNewAddressProofs = 67,
    GetMultipleNewAddressProofsV2 = 68,
    GetNftEditions = 69,
    GetPriorityFeeEstimate = 70,
    GetProgramAccounts = 71,
    GetProgramAccountsV2 = 72,
    GetRecentPerformanceSamples = 73,
    GetRecentPrioritizationFees = 74,
    GetSignatureStatuses = 75,
    GetSignaturesForAddress = 76,
    GetSignaturesForAsset = 77,
    GetSlot = 78,
    GetSlotLeader = 79,
    GetSlotLeaders = 80,
    GetStakeActivation = 81,
    GetStakeMinimumDelegation = 82,
    GetSupply = 83,
    GetTipAccounts = 84,
    GetTokenAccountBalance = 85,
    GetTokenAccounts = 86,
    GetTokenAccountsByDelegate = 87,
    GetTokenAccountsByOwner = 88,
    GetTokenLargestAccounts = 89,
    GetTokenSupply = 90,
    GetTransaction = 91,
    GetTransactionCount = 92,
    GetTransactionWithCompressionInfo = 93,
    GetTransactionsForAddress = 94,
    GetTransfersByAddress = 95,
    GetValidityProof = 96,
    GetValidityProofV2 = 97,
    GetVersion = 98,
    GetVoteAccounts = 99,
    IsBlockhashValid = 100,
    MinimumLedgerSlot = 101,
    RequestAirdrop = 102,
    SearchAssets = 103,
    SendBundle = 104,
    SendStakedTransaction = 105,
    SendTransaction = 106,
    SignMessage = 107,
    SignTransaction = 108,
    SimulateBundle = 109,
    SimulateTransaction = 110,
    TokenTransfers = 111,
    UpdateWebhook = 112,
    WalletBalances = 113,
    WalletHistory = 114,
    WalletIdentity = 115,
    WebhookEvent = 116,
    Count,
}

#[must_use]
pub fn get_standard_method_id(method_bytes: &[u8]) -> usize {
    (match method_bytes {
        b"StakedConnections" => RpcMethod::StakedConnections,
        b"batchIdentityLookup" => RpcMethod::BatchIdentityLookup,
        b"createWebhook" => RpcMethod::CreateWebhook,
        b"deleteWebhook" => RpcMethod::DeleteWebhook,
        b"getAccountInfo" => RpcMethod::GetAccountInfo,
        b"getAsset" => RpcMethod::GetAsset,
        b"getAssetBatch" => RpcMethod::GetAssetBatch,
        b"getAssetProof" => RpcMethod::GetAssetProof,
        b"getAssetProofBatch" => RpcMethod::GetAssetProofBatch,
        b"getAssetProofs" => RpcMethod::GetAssetProofs,
        b"getAssetSignatures" => RpcMethod::GetAssetSignatures,
        b"getAssets" => RpcMethod::GetAssets,
        b"getAssetsByAuthority" => RpcMethod::GetAssetsByAuthority,
        b"getAssetsByCreator" => RpcMethod::GetAssetsByCreator,
        b"getAssetsByGroup" => RpcMethod::GetAssetsByGroup,
        b"getAssetsByOwner" => RpcMethod::GetAssetsByOwner,
        b"getBalance" => RpcMethod::GetBalance,
        b"getBlock" => RpcMethod::GetBlock,
        b"getBlockCommitment" => RpcMethod::GetBlockCommitment,
        b"getBlockHeight" => RpcMethod::GetBlockHeight,
        b"getBlockProduction" => RpcMethod::GetBlockProduction,
        b"getBlockTime" => RpcMethod::GetBlockTime,
        b"getBlocks" => RpcMethod::GetBlocks,
        b"getBlocksWithLimit" => RpcMethod::GetBlocksWithLimit,
        b"getBundleStatuses" => RpcMethod::GetBundleStatuses,
        b"getClusterNodes" => RpcMethod::GetClusterNodes,
        b"getCompressedAccount" => RpcMethod::GetCompressedAccount,
        b"getCompressedAccountProof" => RpcMethod::GetCompressedAccountProof,
        b"getCompressedAccountsByOwner" => RpcMethod::GetCompressedAccountsByOwner,
        b"getCompressedBalance" => RpcMethod::GetCompressedBalance,
        b"getCompressedBalanceByOwner" => RpcMethod::GetCompressedBalanceByOwner,
        b"getCompressedMintTokenHolders" => RpcMethod::GetCompressedMintTokenHolders,
        b"getCompressedTokenAccountBalance" => RpcMethod::GetCompressedTokenAccountBalance,
        b"getCompressedTokenAccountsByDelegate" => RpcMethod::GetCompressedTokenAccountsByDelegate,
        b"getCompressedTokenAccountsByOwner" => RpcMethod::GetCompressedTokenAccountsByOwner,
        b"getCompressedTokenBalancesByOwner" => RpcMethod::GetCompressedTokenBalancesByOwner,
        b"getCompressedTokenBalancesByOwnerV2" => RpcMethod::GetCompressedTokenBalancesByOwnerV2,
        b"getCompressionSignaturesForAccount" => RpcMethod::GetCompressionSignaturesForAccount,
        b"getCompressionSignaturesForAddress" => RpcMethod::GetCompressionSignaturesForAddress,
        b"getCompressionSignaturesForOwner" => RpcMethod::GetCompressionSignaturesForOwner,
        b"getCompressionSignaturesForTokenOwner" => {
            RpcMethod::GetCompressionSignaturesForTokenOwner
        }
        b"getEpochInfo" => RpcMethod::GetEpochInfo,
        b"getEpochSchedule" => RpcMethod::GetEpochSchedule,
        b"getFeeForMessage" => RpcMethod::GetFeeForMessage,
        b"getFirstAvailableBlock" => RpcMethod::GetFirstAvailableBlock,
        b"getGenesisHash" => RpcMethod::GetGenesisHash,
        b"getHealth" => RpcMethod::GetHealth,
        b"getHighestSnapshotSlot" => RpcMethod::GetHighestSnapshotSlot,
        b"getIdentity" => RpcMethod::GetIdentity,
        b"getIndexerHealth" => RpcMethod::GetIndexerHealth,
        b"getIndexerSlot" => RpcMethod::GetIndexerSlot,
        b"getInflationGovernor" => RpcMethod::GetInflationGovernor,
        b"getInflationRate" => RpcMethod::GetInflationRate,
        b"getInflationReward" => RpcMethod::GetInflationReward,
        b"getInflightBundleStatuses" => RpcMethod::GetInflightBundleStatuses,
        b"getLargestAccounts" => RpcMethod::GetLargestAccounts,
        b"getLatestBlockhash" => RpcMethod::GetLatestBlockhash,
        b"getLatestCompressionSignatures" => RpcMethod::GetLatestCompressionSignatures,
        b"getLatestNonVotingSignatures" => RpcMethod::GetLatestNonVotingSignatures,
        b"getLeaderSchedule" => RpcMethod::GetLeaderSchedule,
        b"getMaxRetransmitSlot" => RpcMethod::GetMaxRetransmitSlot,
        b"getMaxShredInsertSlot" => RpcMethod::GetMaxShredInsertSlot,
        b"getMinimumBalanceForRentExemption" => RpcMethod::GetMinimumBalanceForRentExemption,
        b"getMultipleAccounts" => RpcMethod::GetMultipleAccounts,
        b"getMultipleCompressedAccountProofs" => RpcMethod::GetMultipleCompressedAccountProofs,
        b"getMultipleCompressedAccounts" => RpcMethod::GetMultipleCompressedAccounts,
        b"getMultipleNewAddressProofs" => RpcMethod::GetMultipleNewAddressProofs,
        b"getMultipleNewAddressProofsV2" => RpcMethod::GetMultipleNewAddressProofsV2,
        b"getNftEditions" => RpcMethod::GetNftEditions,
        b"getPriorityFeeEstimate" => RpcMethod::GetPriorityFeeEstimate,
        b"getProgramAccounts" => RpcMethod::GetProgramAccounts,
        b"getProgramAccountsV2" => RpcMethod::GetProgramAccountsV2,
        b"getRecentPerformanceSamples" => RpcMethod::GetRecentPerformanceSamples,
        b"getRecentPrioritizationFees" => RpcMethod::GetRecentPrioritizationFees,
        b"getSignatureStatuses" => RpcMethod::GetSignatureStatuses,
        b"getSignaturesForAddress" => RpcMethod::GetSignaturesForAddress,
        b"getSignaturesForAsset" => RpcMethod::GetSignaturesForAsset,
        b"getSlot" => RpcMethod::GetSlot,
        b"getSlotLeader" => RpcMethod::GetSlotLeader,
        b"getSlotLeaders" => RpcMethod::GetSlotLeaders,
        b"getStakeActivation" => RpcMethod::GetStakeActivation,
        b"getStakeMinimumDelegation" => RpcMethod::GetStakeMinimumDelegation,
        b"getSupply" => RpcMethod::GetSupply,
        b"getTipAccounts" => RpcMethod::GetTipAccounts,
        b"getTokenAccountBalance" => RpcMethod::GetTokenAccountBalance,
        b"getTokenAccounts" => RpcMethod::GetTokenAccounts,
        b"getTokenAccountsByDelegate" => RpcMethod::GetTokenAccountsByDelegate,
        b"getTokenAccountsByOwner" => RpcMethod::GetTokenAccountsByOwner,
        b"getTokenLargestAccounts" => RpcMethod::GetTokenLargestAccounts,
        b"getTokenSupply" => RpcMethod::GetTokenSupply,
        b"getTransaction" => RpcMethod::GetTransaction,
        b"getTransactionCount" => RpcMethod::GetTransactionCount,
        b"getTransactionWithCompressionInfo" => RpcMethod::GetTransactionWithCompressionInfo,
        b"getTransactionsForAddress" => RpcMethod::GetTransactionsForAddress,
        b"getTransfersByAddress" => RpcMethod::GetTransfersByAddress,
        b"getValidityProof" => RpcMethod::GetValidityProof,
        b"getValidityProofV2" => RpcMethod::GetValidityProofV2,
        b"getVersion" => RpcMethod::GetVersion,
        b"getVoteAccounts" => RpcMethod::GetVoteAccounts,
        b"isBlockhashValid" => RpcMethod::IsBlockhashValid,
        b"minimumLedgerSlot" => RpcMethod::MinimumLedgerSlot,
        b"requestAirdrop" => RpcMethod::RequestAirdrop,
        b"searchAssets" => RpcMethod::SearchAssets,
        b"sendBundle" => RpcMethod::SendBundle,
        b"sendStakedTransaction" => RpcMethod::SendStakedTransaction,
        b"sendTransaction" => RpcMethod::SendTransaction,
        b"signMessage" => RpcMethod::SignMessage,
        b"signTransaction" => RpcMethod::SignTransaction,
        b"simulateBundle" => RpcMethod::SimulateBundle,
        b"simulateTransaction" => RpcMethod::SimulateTransaction,
        b"tokenTransfers" => RpcMethod::TokenTransfers,
        b"updateWebhook" => RpcMethod::UpdateWebhook,
        b"walletBalances" => RpcMethod::WalletBalances,
        b"walletHistory" => RpcMethod::WalletHistory,
        b"walletIdentity" => RpcMethod::WalletIdentity,
        b"webhookEvent" => RpcMethod::WebhookEvent,
        _ => RpcMethod::Unknown,
    } as usize)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::provider::pricing_parser::load_from_dir;
    use std::collections::{HashMap, HashSet};
    use std::path::Path;

    const PROVIDER_CONFIG_DIR: &str =
        concat!(env!("CARGO_MANIFEST_DIR"), "/config/provider_config");

    fn id(name: &str) -> usize {
        get_standard_method_id(name.as_bytes())
    }

    /// Every method name priced by a shipped provider config, per provider.
    /// `Unknown` is filtered out: it is a real key in helius.toml and is meant
    /// to resolve to slot 0.
    fn provider_method_names() -> HashMap<String, Vec<String>> {
        load_from_dir(Path::new(PROVIDER_CONFIG_DIR))
            .expect("shipped provider configs must parse")
            .into_iter()
            .map(|(provider, methods)| {
                let mut names: Vec<String> = methods
                    .into_keys()
                    .filter(|name| name != "Unknown")
                    .collect();
                names.sort();
                (provider, names)
            })
            .collect()
    }

    #[test]
    fn every_method_in_the_shipped_configs_resolves() {
        // A name the lookup does not know resolves to slot 0, which
        // `ProviderCostTable::new` refuses to price — so the provider silently
        // loses the ability to serve that method.
        let providers = provider_method_names();
        assert!(!providers.is_empty(), "no provider configs were loaded");

        for (provider, names) in providers {
            assert!(!names.is_empty(), "{provider} priced no methods");

            for name in names {
                assert_ne!(
                    id(&name),
                    RpcMethod::Unknown as usize,
                    "{provider} prices `{name}`, but the method lookup does not know it"
                );
            }
        }
    }

    #[test]
    fn distinct_method_names_never_share_an_id() {
        // Guards the hand-written match against copy-paste: two arms pointing
        // at the same variant would make one method inherit the other's price.
        let mut seen: HashMap<usize, String> = HashMap::new();

        for names in provider_method_names().into_values() {
            for name in names {
                let method_id = id(&name);

                if let Some(previous) = seen.insert(method_id, name.clone()) {
                    assert_eq!(
                        previous, name,
                        "`{previous}` and `{name}` both resolve to id {method_id}"
                    );
                }
            }
        }
    }

    #[test]
    fn every_resolved_id_fits_the_cost_table() {
        // `ProviderCostTable` indexes a fixed [u32; Count] array with these ids.
        for names in provider_method_names().into_values() {
            for name in names {
                assert!(
                    id(&name) < RpcMethod::Count as usize,
                    "`{name}` resolves outside the cost table"
                );
            }
        }
    }

    #[test]
    fn known_methods_map_to_expected_variants() {
        // Transcribed from the Solana / Helius / Jito RPC docs rather than from
        // the match above, so a wrong arm shows up as a mismatch here.
        let expected = [
            ("getAccountInfo", RpcMethod::GetAccountInfo as usize),
            ("getBalance", RpcMethod::GetBalance as usize),
            ("getBlockHeight", RpcMethod::GetBlockHeight as usize),
            ("getEpochInfo", RpcMethod::GetEpochInfo as usize),
            ("getHealth", RpcMethod::GetHealth as usize),
            ("getLatestBlockhash", RpcMethod::GetLatestBlockhash as usize),
            (
                "getMultipleAccounts",
                RpcMethod::GetMultipleAccounts as usize,
            ),
            ("getProgramAccounts", RpcMethod::GetProgramAccounts as usize),
            (
                "getSignatureStatuses",
                RpcMethod::GetSignatureStatuses as usize,
            ),
            (
                "getSignaturesForAddress",
                RpcMethod::GetSignaturesForAddress as usize,
            ),
            ("getSlot", RpcMethod::GetSlot as usize),
            (
                "getTokenAccountsByOwner",
                RpcMethod::GetTokenAccountsByOwner as usize,
            ),
            ("getTransaction", RpcMethod::GetTransaction as usize),
            (
                "getTransactionCount",
                RpcMethod::GetTransactionCount as usize,
            ),
            ("sendTransaction", RpcMethod::SendTransaction as usize),
            (
                "simulateTransaction",
                RpcMethod::SimulateTransaction as usize,
            ),
            ("getAsset", RpcMethod::GetAsset as usize),
            ("getAssetsByOwner", RpcMethod::GetAssetsByOwner as usize),
            (
                "getPriorityFeeEstimate",
                RpcMethod::GetPriorityFeeEstimate as usize,
            ),
            ("sendBundle", RpcMethod::SendBundle as usize),
            ("getTipAccounts", RpcMethod::GetTipAccounts as usize),
        ];

        for (name, expected_id) in expected {
            assert_eq!(id(name), expected_id, "wrong id for `{name}`");
        }

        // The table itself must not contain duplicates, or the assertions above
        // would pass while hiding a collision.
        let unique: HashSet<usize> = expected.iter().map(|(_, method_id)| *method_id).collect();
        assert_eq!(unique.len(), expected.len());
    }

    #[test]
    fn unrecognized_names_resolve_to_unknown() {
        for name in [
            "",
            "fooBar",
            "getBalanace",     // typo
            "GETBALANCE",      // lookup is case-sensitive
            "getbalance",      //
            " getBalance",     // untrimmed
            "getBalance ",     //
            "eth_getBalance",  // wrong chain
            "getBalance\u{0}", // trailing NUL
        ] {
            assert_eq!(
                id(name),
                RpcMethod::Unknown as usize,
                "`{name}` should not resolve"
            );
        }
    }
}
