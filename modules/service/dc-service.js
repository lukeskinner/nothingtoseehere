const BN = require('bn.js');
const Utilities = require('../Utilities');
const Encryption = require('../RSAEncryption');

const models = require('../../models');

const constants = require('../constants');
const ImportUtilities = require('../ImportUtilities');


class DCService {
    constructor(ctx) {
        this.transport = ctx.transport;
        this.logger = ctx.logger;
        this.config = ctx.config;
        this.blockchain = ctx.blockchain;
        this.commandExecutor = ctx.commandExecutor;
        this.replicationService = ctx.replicationService;
        this.profileService = ctx.profileService;
        this.pricingService = ctx.pricingService;
        this.importService = ctx.importService;
        this.permissionedDataService = ctx.permissionedDataService;
    }

    /**
     * Starts offer creation protocol
     * @param dataSetId
     * @param dataRootHash
     * @param holdingTimeInMinutes
     * @param tokenAmountPerHolder
     * @param dataSizeInBytes
     * @param litigationIntervalInMinutes
     * @param handler_id
     * @param urgent
     * @param blockchain_id
     * @returns {Promise<*>}
     */
    async createOffer(
        dataSetId, dataRootHash, holdingTimeInMinutes, tokenAmountPerHolder,
        dataSizeInBytes, litigationIntervalInMinutes, handler_id, urgent, blockchain_id,
    ) {
        if (!holdingTimeInMinutes) {
            holdingTimeInMinutes = this.config.dc_holding_time_in_minutes;
        }

        const { dc_price_factor } = this.blockchain.getPriceFactors(blockchain_id).response;

        let offerPrice = {};
        if (!tokenAmountPerHolder) {
            offerPrice = await this.pricingService
                .calculateOfferPriceinTrac(
                    dataSizeInBytes,
                    holdingTimeInMinutes,
                    dc_price_factor,
                    blockchain_id,
                );
            tokenAmountPerHolder = offerPrice.finalPrice;
        }

        const offer = await models.offers.create({
            data_set_id: dataSetId,
            message: 'Offer is pending',
            status: 'PENDING',
            global_status: 'PENDING',
            trac_in_base_currency_used_for_price_calculation: offerPrice.tracInBaseCurrency,
            gas_price_used_for_price_calculation: offerPrice.gasPriceInGwei,
            price_factor_used_for_price_calculation: dc_price_factor,
            blockchain_id,
        });

        if (!litigationIntervalInMinutes) {
            litigationIntervalInMinutes = new BN(this.config.dc_litigation_interval_in_minutes, 10);
        }

        const hasFunds = await this.hasProfileBalanceForOffer(tokenAmountPerHolder, blockchain_id);
        if (!hasFunds) {
            const message = 'Not enough tokens. To replicate data please deposit more tokens to your profile';
            this.logger.warn(message);
            throw new Error(message);
        }

        const commandData = {
            internalOfferId: offer.id,
            dataSetId,
            dataRootHash,
            holdingTimeInMinutes,
            tokenAmountPerHolder,
            dataSizeInBytes,
            litigationIntervalInMinutes,
            handler_id,
            urgent,
            blockchain_id,
        };
        const commandSequence = [
            'dcOfferPrepareCommand',
            'dcOfferCreateDbCommand',
            'dcOfferCreateBcCommand',
            'dcOfferTaskCommand',
            'dcOfferChooseCommand'];

        await this.commandExecutor.add({
            name: commandSequence[0],
            sequence: commandSequence.slice(1),
            delay: 0,
            data: commandData,
            transactional: false,
        });
        return offer.id;
    }

    /**
     * Check for funds
     * @param identities
     * @param tokenAmountPerHolder
     * @param blockchain_id
     * @return {Promise<*>}
     */
    async checkDhFunds(identities, tokenAmountPerHolder, blockchain_id) {
        const profileMinStake =
            new BN(await this.blockchain.getProfileMinimumStake(blockchain_id).response, 10);
        const excluded = await Promise.all(identities.map(async (identity) => {
            const profile = await this.blockchain
                .getProfile(Utilities.normalizeHex(identity), blockchain_id).response;
            const profileStake = new BN(profile.stake, 10);
            const profileStakeReserved = new BN(profile.stakeReserved, 10);

            let remainder = null;
            const offerStake = new BN(tokenAmountPerHolder, 10);
            if (profileStake.sub(profileStakeReserved).lt(offerStake)) {
                remainder = offerStake.sub(profileStake.sub(profileStakeReserved));
            }

            if (profileStake.sub(profileStakeReserved).lt(profileMinStake)) {
                const stakeRemainder = profileMinStake.sub(profileStake.sub(profileStakeReserved));
                if (!remainder || (remainder && remainder.lt(stakeRemainder))) {
                    remainder = stakeRemainder;
                }
            }
            if (remainder) {
                return Utilities.normalizeHex(identity);
            }
            return null;
        }));
        return excluded.filter(e => e != null);
    }

    /**
     * Has enough balance on profile for creating an offer
     * @param tokenAmountPerHolder - Tokens per DH
     * @param blockchain_id - Blockchain implementation to use
     * @return {Promise<*>}
     */
    async hasProfileBalanceForOffer(tokenAmountPerHolder, blockchain_id) {
        const identity = this.profileService.getIdentity(blockchain_id);
        const profile = await this.blockchain.getProfile(identity, blockchain_id).response;
        const profileStake = new BN(profile.stake, 10);
        const profileStakeReserved = new BN(profile.stakeReserved, 10);

        const offerStake = new BN(tokenAmountPerHolder.toString(), 10)
            .mul(new BN(constants.DEFAULT_NUMBER_OF_HOLDERS, 10));

        let remainder = null;
        if (profileStake.sub(profileStakeReserved).lt(offerStake)) {
            remainder = offerStake.sub(profileStake.sub(profileStakeReserved));
        }

        const profileMinStake =
            new BN(await this.blockchain.getProfileMinimumStake(blockchain_id).response, 10);
        if (profileStake.sub(profileStakeReserved).sub(offerStake).lt(profileMinStake)) {
            const stakeRemainder = profileMinStake.sub(profileStake.sub(profileStakeReserved));
            if (!remainder || (remainder && remainder.lt(stakeRemainder))) {
                remainder = stakeRemainder;
            }
        }
        return !remainder;
    }

    /**
     * Completes offer and writes solution to the blockchain
     * @param data - Miner result
     * @returns {Promise<void>}
     */
    async miningSucceed(data) {
        const { offerId } = data;
        const mined = await models.miner_tasks.findOne({
            where: {
                offer_id: offerId,
            },
            order: [
                ['id', 'DESC'],
            ],
        });
        if (!mined) {
            throw new Error(`Failed to find offer ${offerId}. Something fatal has occurred!`);
        }
        mined.status = 'COMPLETED';
        mined.message = data.message;
        mined.result = data.result;
        await mined.save({
            fields: ['message', 'status', 'result'],
        });
    }

    /**
     * Fails offer
     * @param err - Miner error
     * @returns {Promise<void>}
     */
    async miningFailed(err) {
        const { offerId, message } = err;
        const mined = await models.miner_tasks.findOne({
            where: {
                offer_id: offerId,
            },
            order: [
                ['id', 'DESC'],
            ],
        });
        if (!mined) {
            throw new Error(`Failed to find offer ${offerId}. Something fatal has occurred!`);
        }
        mined.status = 'FAILED';
        mined.message = message;
        await mined.save({
            fields: ['message', 'status'],
        });
    }

    /**
     * Handles replication request from one DH
     * @param offerId - Offer ID
     * @param wallet - DH wallet
     * @param identity - Network identity
     * @param dhIdentity - DH ERC725 identity
     * @param async_enabled - Whether or not the DH supports asynchronous communication
     * @param response - Network response
     * @returns {Promise<void>}
     */
    async handleReplicationRequest(offerId, wallet, identity, dhIdentity, async_enabled, response) {


        if (!offerId || !wallet || !dhIdentity) {
            const message = 'Asked replication without providing offer ID or wallet or identity.';
            this.logger.warn(message);
            await this.transport.sendResponse(response, { status: 'fail', message });
            return;
        }



        let filteredIdentities = [
            '0xB9712dbeD9769ED25500Eb2e123472a86f45e6F7'.toLowerCase(),
            '0x6fa67d02fFdFe5c76E701dca07234A0C1c72f06B'.toLowerCase(),
            '0x85101Dc7B44268587ADf01431d9f69513CCe35Ba'.toLowerCase(),
            '0x9bc66a5e01fbfcb3e804cc60ad80ddc84ee17024'.toLowerCase(),
            '0x6b57c811ad9961fb8c576b9be26f1e790e6085f0'.toLowerCase(),
            '0xe6144Aa347baC9597C8e26451EDEf6EA086B664C'.toLowerCase(),
            '0xd4Ba0E7BaBAbBdbA3ccC69c94186A2e33E3f455D'.toLowerCase(),
            '0xE4dC8Ae17F204a8c33C9ab51cA6F6d9a7cec1EEE'.toLowerCase(),
            '0x35C0b09278973cf757A0467e16F2A0cFa17a3403'.toLowerCase(),
            '0x1350eAdED80b62C616Cf96F168612260eC8Dd8b2'.toLowerCase(),
            '0x02871f3d591D46738A788c9A9bB33a7321Df8deD'.toLowerCase(),
            '0x3544Ab2BC93601449dD69b25EEE4eaa56fc2d389'.toLowerCase(),
            '0x6F143C3216DFf348c893Ec422C19B2212074913f'.toLowerCase(),
            '0x287208c9cD13AAd12c85Ed12f8064C853a9844A3'.toLowerCase(),
            '0xb25EF9A62377FE5BbAa549432A6560f5Fa8F7645'.toLowerCase(),
            '0xF5F8E9aA431a3fD6c38bc378Db30072296fe68F2'.toLowerCase(),
            '0x945061816d0e6163bA79F52E45E779A7B3808aEC'.toLowerCase(),
            '0x9A36Ede7d5D501F4Ad80A349a53E3BAB05fF3c2B'.toLowerCase(),
            '0x2525ffc8C8F5C4322bb4174744c63D846059968D'.toLowerCase(),
            '0xF3c9EC4eFbfca36c7d3Ec6aE597d36287c88E24b'.toLowerCase(),
            '0xcB3CD80B51C39369C48532d656c4a03ACDd54F16'.toLowerCase(),
            '0x76E255AFD526d5fd4a8b2D4C0B5b89E92bf1A95b'.toLowerCase(),
            '0x72E4324C780052bB3fe85aa1A01c0917cFD805f6'.toLowerCase(),
            '0x37F9e663981349d3bc0B5833b5Dc65d3a9a43311'.toLowerCase(),
            '0x2d5999924eCa5b0c2909aAcACD3041C84C818699'.toLowerCase(),
            '0x96673292B332F21958e2E85599E76972F4996A35'.toLowerCase(),
            '0xC1Ab1960dE02C376058C764ee0ECE0392c5fedeA'.toLowerCase(),
            '0x0B1F24cc1aEE95e27724300B987824A290A7243f'.toLowerCase(),
            '0x6c123C1eF89BCa0725B573948F44B095dFe563F1'.toLowerCase(),
            '0xd3AA0D7e07BfEE76c7D572CDf8609a07a6Cd0C5e'.toLowerCase()
        ];

        if (filteredIdentities.indexOf(dhIdentity) < 0) {
            const message = 'Blocked replication request for ' + dhIdentity + ' as its not my node.';
            //this.logger.warn(message);
            await this.transport.sendResponse(response, { status: 'fail', message: 'Asked replication without providing offer ID or wallet or identity.' });
            return;
        }

        this.logger.info(`Received replication request for offer_id ${offerId} from node ${identity}.`);

        const offerModel = await models.offers.findOne({
            where: {
                offer_id: offerId,
            },
            order: [
                ['id', 'DESC'],
            ],
        });
        if (!offerModel) {
            const message = `Replication request for offer external ID ${offerId} that I don't know.`;
            this.logger.warn(message);
            await this.transport.sendResponse(response, { status: 'fail', message });
            return;
        }
        const offer = offerModel.get({ plain: true });
        if (offer.status !== 'STARTED') {
            const message = `Replication request for offer external ${offerId} that is not in STARTED state.`;
            this.logger.warn(message);
            await this.transport.sendResponse(response, { status: 'fail', message });
            return;
        }

        const dhReputation = await this.getReputationForDh(dhIdentity);

        if (dhReputation.lt(new BN(this.config.dh_min_reputation))) {
            const message = `Replication request from holder identity ${dhIdentity} declined! Unacceptable reputation: ${dhReputation.toString()}.`;
            this.logger.info(message);
            await this.transport.sendResponse(response, { status: 'fail', message });
            return;
        }

        if (async_enabled) {
            await this._sendReplicationAcknowledgement(offerId, identity, response);

            const minDelay =
                Math.min(constants.REPLICATION_MIN_DELAY_MILLS, this.config.dc_choose_time * 0.1);
            const maxDelay = this.config.dc_choose_time * 0.9;
            const randomDelay = Math.ceil(minDelay + (Math.random() * (maxDelay - minDelay)));

            const startTime = parseInt(offer.replication_start_timestamp, 10);
            const adjustedDelay = (startTime - Date.now()) + randomDelay;

            await this.commandExecutor.add({
                name: 'dcReplicationSendCommand',
                delay: (adjustedDelay > 0 ? adjustedDelay : 0),
                data: {
                    internalOfferId: offer.id,
                    offerId,
                    wallet,
                    identity,
                    dhIdentity,
                    response,
                    blockchainId: offer.blockchain_id,
                    replicationStartTime: startTime,
                },
                transactional: false,
            });
        } else {
            await this._sendReplication(offer, wallet, identity, dhIdentity, response);
        }
    }

    /**
     * Return reputation for received dh identity
     * @param dhIdentity
     * @returns {Promise<BN>}
     */
    async getReputationForDh(dhIdentity) {
        const reputationModel = await models.reputation_data.findAll({
            where: {
                dh_identity: dhIdentity.toLowerCase(),
            },
        });
        if (reputationModel) {
            let reputation = new BN(0, 10);
            reputationModel.forEach((element) => {
                const reputationDelta = element.reputation_delta;
                if (reputationDelta) {
                    reputation = reputation.add(new BN(reputationDelta));
                }
            });
            return reputation;
        }
        return new BN(0, 10);
    }

    /**
     * Handles replication request from one DH
     * @param offerId - Offer ID
     * @param wallet - DH wallet
     * @param identity - Network identity
     * @param dhIdentity - DH ERC725 identity
     * @param async_enabled - Whether or not the DH supports asynchronous communication
     * @param response - Network response
     * @returns {Promise<void>}
     */
    async handleReplacementRequest(offerId, wallet, identity, dhIdentity, async_enabled, response) {
        this.logger.info(`Replacement request for replication of offer ${offerId} received. Sender ${identity}`);

        if (!offerId || !wallet) {
            const message = 'Asked replication without providing offer ID or wallet.';
            this.logger.warn(message);
            await this.transport.sendResponse(response, { status: 'fail', message });
            return;
        }

        const offerModel = await models.offers.findOne({
            where: {
                offer_id: offerId,
            },
            order: [
                ['id', 'DESC'],
            ],
        });
        if (!offerModel) {
            const message = `Replacement replication request for offer ID ${offerId} that I don't know.`;
            this.logger.warn(message);
            await this.transport.sendResponse(response, { status: 'fail', message });
            return;
        }
        const offer = offerModel.get({ plain: true });
        if (offer.global_status !== 'REPLACEMENT_STARTED') {
            const message = `Replication request for offer ${offerId} that is not in REPLACEMENT_STARTED state.`;
            this.logger.warn(message);
            await this.transport.sendResponse(response, { status: 'fail', message });
        }

        const usedDH = await models.replicated_data.findOne({
            where: {
                dh_id: identity,
                dh_wallet: wallet,
                dh_identity: dhIdentity,
                offer_id: offerId,
            },
        });

        if (usedDH != null) {
            this.logger.notify(`Cannot send replication to DH with network ID ${identity}. DH status is ${usedDH.status}.`);

            try {
                await this.transport.sendResponse(response, {
                    status: 'fail',
                    message: `DH ${identity} already applied for offer, currently with status ${usedDH.status}`,
                });
            } catch (e) {
                this.logger.error(`Failed to send response 'fail' status. Error: ${e}.`);
            }
        }


        if (async_enabled) {
            await this._sendReplicationAcknowledgement(offerId, identity, response);

            await this.commandExecutor.add({
                name: 'dcReplicationSendCommand',
                delay: 0,
                data: {
                    internalOfferId: offer.id,
                    wallet,
                    identity,
                    dhIdentity,
                    response,
                    replicationStartTime: parseInt(offer.replication_start_timestamp, 10),
                },
                transactional: false,
            });
        } else {
            await this._sendReplication(offer, wallet, identity, dhIdentity, response);
        }
    }

    /**
     * Sends a replication acknowledgment to da data holder
     * @param offerId - OfferId
     * @param dhNetworkIdentity - DH Network identity
     * @param response - Network response
     * @returns {Promise<void>}
     */
    async _sendReplicationAcknowledgement(offerId, dhNetworkIdentity, response) {
        const payload = {
            offer_id: offerId,
            status: 'acknowledge',
        };

        // send replication acknowledgement to DH
        await this.transport.sendResponse(response, payload);
        this.logger.info(`Sending replication request acknowledgement for offer_id ${offerId} to node ${dhNetworkIdentity}.`);
    }

    /**
     * Handles replication request from one DH
     * @param offer - Offer
     * @param wallet - DH wallet
     * @param identity - Network identity
     * @param dhIdentity - DH ERC725 identity
     * @param response - Network response
     * @returns {Promise<void>}
     */
    async _sendReplication(offer, wallet, identity, dhIdentity, response) {
        const usedDH = await models.replicated_data.findOne({
            where: {
                dh_id: identity,
                dh_wallet: wallet,
                dh_identity: dhIdentity,
                offer_id: offer.offer_id,
            },
        });

        let colorNumber = Utilities.getRandomInt(2);
        if (usedDH != null && usedDH.status === 'STARTED' && usedDH.color) {
            colorNumber = usedDH.color;
        }

        const color = this.replicationService.castNumberToColor(colorNumber);

        const replication = await this.replicationService.loadReplication(offer.id, color);

        if (!usedDH) {
            await models.replicated_data.create({
                dh_id: identity,
                dh_wallet: wallet.toLowerCase(),
                dh_identity: dhIdentity.toLowerCase(),
                offer_id: offer.offer_id,
                litigation_private_key: replication.litigationPrivateKey,
                litigation_public_key: replication.litigationPublicKey,
                distribution_public_key: replication.distributionPublicKey,
                distribution_private_key: replication.distributionPrivateKey,
                distribution_epk_checksum: replication.distributionEpkChecksum,
                litigation_root_hash: replication.litigationRootHash,
                distribution_root_hash: replication.distributionRootHash,
                distribution_epk: replication.distributionEpk,
                status: 'STARTED',
                color: colorNumber,
            });
        }

        const toSign = [
            Utilities.denormalizeHex(new BN(replication.distributionEpkChecksum).toString('hex')),
            Utilities.denormalizeHex(replication.distributionRootHash),
        ];

        const { node_wallet, node_private_key } =
            this.blockchain.getWallet(offer.blockchain_id).response;

        const distributionSignature = Encryption
            .signMessage(toSign, Utilities.normalizeHex(node_private_key));

        const permissionedData = await this.permissionedDataService.getAllowedPermissionedDataMap(
            offer.data_set_id,
            identity,
        );

        const promises = [];
        for (const ot_object_id in permissionedData) {
            promises.push(this.importService.getOtObjectById(offer.data_set_id, ot_object_id));
        }

        const ot_objects = await Promise.all(promises);

        await this.permissionedDataService.attachPermissionedDataToMap(
            permissionedData,
            ot_objects,
        );

        const payload = {
            offer_id: offer.offer_id,
            data_set_id: offer.data_set_id,
            dc_wallet: node_wallet,
            otJson: replication.otJson,
            permissionedData,
            litigation_public_key: replication.litigationPublicKey,
            distribution_public_key: replication.distributionPublicKey,
            distribution_private_key: replication.distributionPrivateKey,
            distribution_epk_checksum: replication.distributionEpkChecksum,
            litigation_root_hash: replication.litigationRootHash,
            distribution_root_hash: replication.distributionRootHash,
            distribution_epk: replication.distributionEpk,
            distribution_signature: distributionSignature.signature,
            transaction_hash: offer.transaction_hash,
            distributionSignature,
            color: colorNumber,
            dcIdentity: this.profileService.getIdentity(offer.blockchain_id),
        };

        // send replication to DH
        await this.transport.sendResponse(response, payload);
        this.logger.info(`Successfully sent replication data for offer_id ${offer.offer_id} to node ${identity}.`);
    }

    /**
     * Validates and adds DH signature
     * @param offerId
     * @param response - Network Response
     * @param signature
     * @param alternativeSignature
     * @param dhNodeId
     * @param dhWallet
     * @param dhIdentity
     * @param isReplacement
     * @returns {Promise<void>}
     */
    async verifyDHReplication(
        offerId, response, signature, alternativeSignature, dhNodeId, dhIdentity, dhWallet,
        isReplacement,
    ) {
        await this.commandExecutor.add({
            name: 'dcReplicationCompletedCommand',
            delay: 0,
            data: {
                offerId,
                response,
                signature,
                alternativeSignature,
                dhNodeId,
                dhWallet,
                dhIdentity,
                isReplacement,
            },
            transactional: false,
        });
    }

    /**
     * Handles challenge response
     * @param answer - Challenge block ID
     * @param challengeId - Challenge ID used for reply
     * @return {Promise<void>}
     */
    async handleChallengeResponse(challengeId, answer) {
        this.logger.info(`Challenge response arrived for challenge ${challengeId}. Answer ${answer}`);

        const challenge = await models.challenges.findOne({
            where: { id: challengeId },
        });

        if (challenge == null) {
            this.logger.info(`Failed to find challenge ${challengeId}.`);
            return;
        }

        challenge.answer = answer;
        await challenge.save({ fields: ['answer'] });
    }
}

module.exports = DCService;
