// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
// SPDX-FileCopyrightText: 2024 Canonical Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	"github.com/5GC-DEV/openapi-cdac/models"
	"github.com/omec-project/webconsole/backend/factory"
	"github.com/omec-project/webconsole/backend/logger"
	"github.com/omec-project/webconsole/configmodels"
	"github.com/omec-project/webconsole/dbadapter"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	authSubsDataColl = "subscriptionData.authenticationData.authenticationSubscription"
	amDataColl       = "subscriptionData.provisionedData.amData"
	smDataColl       = "subscriptionData.provisionedData.smData"
	smfSelDataColl   = "subscriptionData.provisionedData.smfSelectionSubscriptionData"
	amPolicyDataColl = "policyData.ues.amData"
	smPolicyDataColl = "policyData.ues.smData"
	flowRuleDataColl = "policyData.ues.flowRule"
	devGroupDataColl = "webconsoleData.snapshots.devGroupData"
	sliceDataColl    = "webconsoleData.snapshots.sliceData"
)

type Update5GSubscriberMsg struct {
	Msg          *configmodels.ConfigMessage
	PrevDevGroup *configmodels.DeviceGroups
	PrevSlice    *configmodels.Slice
}

var (
	execCommand        = exec.Command
	rwLock             sync.RWMutex
	subscriberAuthData SubscriberAuthenticationData
)

func configHandler(configMsgChan chan *configmodels.ConfigMessage, configReceived chan bool) {
	// Start Goroutine which will listens for subscriber config updates
	// and update the mongoDB. Only for 5G
	subsUpdateChan := make(chan *Update5GSubscriberMsg, 10)
	if factory.WebUIConfig.Configuration.Mode5G {
		go Config5GUpdateHandle(subsUpdateChan)
	}
	firstConfigRcvd := firstConfigReceived()
	if firstConfigRcvd {
		configReceived <- true
	}
	for {
		logger.ConfigLog.Infoln("waiting for configuration event")
		configMsg := <-configMsgChan
		if configMsg.MsgType == configmodels.Sub_data {
			imsiVal := strings.ReplaceAll(configMsg.Imsi, "imsi-", "")
			logger.ConfigLog.Infoln("received imsi from config channel:", imsiVal)
			if configMsg.MsgMethod == configmodels.Delete_op {
				handleSubscriberDelete(configMsg.Imsi)
			} else {
				handleSubscriberPost(configMsg.Imsi, configMsg.AuthSubData)
			}
			logger.ConfigLog.Infof("received Imsi [%v] configuration from config channel", configMsg.Imsi)
		}

		if configMsg.MsgMethod == configmodels.Post_op || configMsg.MsgMethod == configmodels.Put_op {
			if !firstConfigRcvd && (configMsg.MsgType == configmodels.Device_group || configMsg.MsgType == configmodels.Network_slice) {
				logger.ConfigLog.Debugln("first config received from ROC")
				firstConfigRcvd = true
				configReceived <- true
			}

			// update config snapshot
			if configMsg.DevGroup != nil {
				logger.ConfigLog.Infof("received Device Group [%v] configuration from config channel", configMsg.DevGroupName)
				handleDeviceGroupPost(configMsg, subsUpdateChan)
			}

			if configMsg.Slice != nil {
				logger.ConfigLog.Infof("received Slice [%v] configuration from config channel", configMsg.SliceName)
				handleNetworkSlicePost(configMsg, subsUpdateChan)
			}

			// loop through all clients and send this message to all clients
			if len(clientNFPool) == 0 {
				logger.ConfigLog.Infoln("no client available. No need to send config")
			}
			for _, client := range clientNFPool {
				logger.ConfigLog.Infoln("push config for client:", client.id)
				client.outStandingPushConfig <- configMsg
			}
		} else {
			if configMsg.MsgType != configmodels.Sub_data {
				// update config snapshot
				if configMsg.DevGroup == nil && configMsg.DevGroupName != "" {
					logger.ConfigLog.Infof("received delete Device Group [%v] from config channel", configMsg.DevGroupName)
					handleDeviceGroupDelete(configMsg, subsUpdateChan)
				}

				if configMsg.Slice == nil && configMsg.SliceName != "" {
					logger.ConfigLog.Infof("received delete Slice [%v] from config channel", configMsg.SliceName)
					handleNetworkSliceDelete(configMsg, subsUpdateChan)
				}
			} else {
				logger.ConfigLog.Infof("received delete Subscriber [%v] from config channel", configMsg.Imsi)
			}
			// loop through all clients and send this message to all clients
			if len(clientNFPool) == 0 {
				logger.ConfigLog.Infoln("no client available. No need to send config")
			}
			for _, client := range clientNFPool {
				logger.ConfigLog.Infoln("push config for client:", client.id)
				client.outStandingPushConfig <- configMsg
			}
		}
	}
}

func handleSubscriberPost(imsi string, authSubData *models.AuthenticationSubscription) {
	rwLock.Lock()
	subscriberAuthData.SubscriberAuthenticationDataCreate(imsi, authSubData)
	rwLock.Unlock()
}

func handleSubscriberDelete(imsi string) {
	rwLock.Lock()
	subscriberAuthData.SubscriberAuthenticationDataDelete(imsi)
	rwLock.Unlock()
}

func handleDeviceGroupPost(configMsg *configmodels.ConfigMessage, subsUpdateChan chan *Update5GSubscriberMsg) {
	rwLock.Lock()
	if factory.WebUIConfig.Configuration.Mode5G {
		var config5gMsg Update5GSubscriberMsg
		config5gMsg.Msg = configMsg
		config5gMsg.PrevDevGroup = getDeviceGroupByName(configMsg.DevGroupName)
		subsUpdateChan <- &config5gMsg
	}
	filter := bson.M{"group-name": configMsg.DevGroupName}
	devGroupDataBsonA := configmodels.ToBsonM(configMsg.DevGroup)
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(devGroupDataColl, filter, devGroupDataBsonA)
	if errPost != nil {
		logger.DbLog.Warnln(errPost)
	}
	rwLock.Unlock()
}

func handleDeviceGroupDelete(configMsg *configmodels.ConfigMessage, subsUpdateChan chan *Update5GSubscriberMsg) {
	rwLock.Lock()
	if factory.WebUIConfig.Configuration.Mode5G {
		var config5gMsg Update5GSubscriberMsg
		config5gMsg.Msg = configMsg
		config5gMsg.PrevDevGroup = getDeviceGroupByName(configMsg.DevGroupName)
		subsUpdateChan <- &config5gMsg
	}
	filter := bson.M{"group-name": configMsg.DevGroupName}
	err := dbadapter.CommonDBClient.RestfulAPIDeleteOne(devGroupDataColl, filter)
	if err != nil {
		logger.DbLog.Warnln(err)
	}
	rwLock.Unlock()
}

func handleNetworkSlicePost(configMsg *configmodels.ConfigMessage, subsUpdateChan chan *Update5GSubscriberMsg) {
	rwLock.Lock()
	if factory.WebUIConfig.Configuration.Mode5G {
		var config5gMsg Update5GSubscriberMsg
		config5gMsg.Msg = configMsg
		config5gMsg.PrevSlice = getSliceByName(configMsg.SliceName)
		subsUpdateChan <- &config5gMsg
	}
	filter := bson.M{"slice-name": configMsg.SliceName}
	sliceDataBsonA := configmodels.ToBsonM(configMsg.Slice)
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(sliceDataColl, filter, sliceDataBsonA)
	if errPost != nil {
		logger.DbLog.Warnln(errPost)
	}
	if factory.WebUIConfig.Configuration.SendPebbleNotifications {
		err := sendPebbleNotification("aetherproject.org/webconsole/networkslice/create")
		if err != nil {
			logger.ConfigLog.Warnf("sending Pebble notification failed: %s. continuing silently", err.Error())
		}
	}
	rwLock.Unlock()
}

func handleNetworkSliceDelete(configMsg *configmodels.ConfigMessage, subsUpdateChan chan *Update5GSubscriberMsg) {
	rwLock.Lock()
	if factory.WebUIConfig.Configuration.Mode5G {
		var config5gMsg Update5GSubscriberMsg
		config5gMsg.Msg = configMsg
		config5gMsg.PrevSlice = getSliceByName(configMsg.SliceName)
		subsUpdateChan <- &config5gMsg
	}
	filter := bson.M{"slice-name": configMsg.SliceName}
	err := dbadapter.CommonDBClient.RestfulAPIDeleteOne(sliceDataColl, filter)
	if err != nil {
		logger.DbLog.Warnln(err)
	}
	if factory.WebUIConfig.Configuration.SendPebbleNotifications {
		err := sendPebbleNotification("aetherproject.org/webconsole/networkslice/delete")
		if err != nil {
			logger.ConfigLog.Warnf("sending Pebble notification failed: %s. continuing silently", err.Error())
		}
	}
	rwLock.Unlock()
}

func firstConfigReceived() bool {
	return len(getDeviceGroups()) > 0 || len(getSlices()) > 0
}

func getDeviceGroups() []*configmodels.DeviceGroups {
	rawDeviceGroups, errGetMany := dbadapter.CommonDBClient.RestfulAPIGetMany(devGroupDataColl, nil)
	if errGetMany != nil {
		logger.DbLog.Warnln(errGetMany)
	}
	var deviceGroups []*configmodels.DeviceGroups
	for _, rawDevGroup := range rawDeviceGroups {
		var devGroupData configmodels.DeviceGroups
		err := json.Unmarshal(configmodels.MapToByte(rawDevGroup), &devGroupData)
		if err != nil {
			logger.DbLog.Errorf("could not unmarshall device group %v", rawDevGroup)
		}
		deviceGroups = append(deviceGroups, &devGroupData)
	}
	return deviceGroups
}

func getDeviceGroupByName(name string) *configmodels.DeviceGroups {
	filter := bson.M{"group-name": name}
	devGroupDataInterface, errGetOne := dbadapter.CommonDBClient.RestfulAPIGetOne(devGroupDataColl, filter)
	if errGetOne != nil {
		logger.DbLog.Warnln(errGetOne)
	}
	var devGroupData configmodels.DeviceGroups
	err := json.Unmarshal(configmodels.MapToByte(devGroupDataInterface), &devGroupData)
	if err != nil {
		logger.DbLog.Errorf("could not unmarshall device group %v", devGroupDataInterface)
	}
	return &devGroupData
}

func getSlices() []*configmodels.Slice {
	rawSlices, errGetMany := dbadapter.CommonDBClient.RestfulAPIGetMany(sliceDataColl, nil)
	if errGetMany != nil {
		logger.DbLog.Warnln(errGetMany)
	}
	var slices []*configmodels.Slice
	for _, rawSlice := range rawSlices {
		var sliceData configmodels.Slice
		err := json.Unmarshal(configmodels.MapToByte(rawSlice), &sliceData)
		if err != nil {
			logger.DbLog.Errorf("could not unmarshall slice %v", rawSlice)
		}
		slices = append(slices, &sliceData)
	}
	return slices
}

func getSliceByName(name string) *configmodels.Slice {
	filter := bson.M{"slice-name": name}
	sliceDataInterface, errGetOne := dbadapter.CommonDBClient.RestfulAPIGetOne(sliceDataColl, filter)
	if errGetOne != nil {
		logger.DbLog.Warnln(errGetOne)
	}
	var sliceData configmodels.Slice
	err := json.Unmarshal(configmodels.MapToByte(sliceDataInterface), &sliceData)
	if err != nil {
		logger.DbLog.Errorf("could not unmarshall slice %v", sliceDataInterface)
	}
	return &sliceData
}

func getAddedImsisList(group, prevGroup *configmodels.DeviceGroups) (aimsis []string) {
	if group == nil {
		return
	}
	for _, imsi := range group.Imsis {
		if prevGroup == nil {
			if subscriberAuthData.SubscriberAuthenticationDataGet("imsi-"+imsi) != nil {
				aimsis = append(aimsis, imsi)
			}
		} else {
			var found bool
			for _, pimsi := range prevGroup.Imsis {
				if pimsi == imsi {
					found = true
				}
			}

			if !found {
				aimsis = append(aimsis, imsi)
			}
		}
	}

	return
}

func getDeletedImsisList(group, prevGroup *configmodels.DeviceGroups) (dimsis []string) {
	if prevGroup == nil {
		return
	}

	if group == nil {
		return prevGroup.Imsis
	}

	for _, pimsi := range prevGroup.Imsis {
		var found bool
		for _, imsi := range group.Imsis {
			if pimsi == imsi {
				found = true
			}
		}

		if !found {
			dimsis = append(dimsis, pimsi)
		}
	}

	return
}

func updateAmPolicyData(imsi string) {
	// ampolicydata
	var amPolicy models.AmPolicyData
	amPolicy.SubscCats = append(amPolicy.SubscCats, "aether")
	amPolicyDatBsonA := configmodels.ToBsonM(amPolicy)
	amPolicyDatBsonA["ueId"] = "imsi-" + imsi
	filter := bson.M{"ueId": "imsi-" + imsi}
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(amPolicyDataColl, filter, amPolicyDatBsonA)
	if errPost != nil {
		logger.DbLog.Warnln(errPost)
	}
}

func updateSmPolicyData(snssai *models.Snssai, dnnMap map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos, imsi string) {
	var smPolicyData models.SmPolicyData
	var smPolicySnssaiData models.SmPolicySnssaiData
	// Iterate over all DNNs in the map
	dnnData := make(map[string]models.SmPolicyDnnData)

	for dnn := range dnnMap { // Extract each DNN from the map
		dnnData[dnn] = models.SmPolicyDnnData{
			Dnn: dnn,
		}
	}
	// smpolicydata
	smPolicySnssaiData.Snssai = snssai
	smPolicySnssaiData.SmPolicyDnnData = dnnData
	smPolicyData.SmPolicySnssaiData = make(map[string]models.SmPolicySnssaiData)
	smPolicyData.SmPolicySnssaiData[SnssaiModelsToHex(*snssai)] = smPolicySnssaiData
	smPolicyDatBsonA := configmodels.ToBsonM(smPolicyData)
	smPolicyDatBsonA["ueId"] = "imsi-" + imsi
	filter := bson.M{"ueId": "imsi-" + imsi}
	logger.DbLog.Infof("Data to be sent to database - smPolicyData: %+v", smPolicyDatBsonA)
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(smPolicyDataColl, filter, smPolicyDatBsonA)
	if errPost != nil {
		logger.DbLog.Warnln(errPost)
	}
}

func updateAmProvisionedData(gpsi string, snssai *models.Snssai, aggregatedQoS configmodels.DeviceGroupsIpDomainExpandedUeDnnQos, mcc, mnc, imsi string) {
	var gpsiSlice []string // Initialize a slice to hold the GPSI.
	if gpsi != "" {        // Only add if gpsi is not empty
		gpsiSlice = []string{gpsi}
	}

	filter := bson.M{
		"ueId":          "imsi-" + imsi,
		"servingPlmnId": mcc + mnc,
	}

	existingRecord, err := dbadapter.CommonDBClient.RestfulAPIGetOne(amDataColl, filter)
	if err != nil && err.Error() != "mongo: no documents in result" {
		logger.DbLog.Warnf("Failed to fetch existing record for ueId: %s, error: %v", imsi, err)
		return
	}

	var snssaiList []models.Snssai
	snssaiList = append(snssaiList, *snssai)

	var amData models.AccessAndMobilitySubscriptionData

	if existingRecord == nil {
		// Construct the AccessAndMobilitySubscriptionData structure with the subscriber's details.
		amData = models.AccessAndMobilitySubscriptionData{
			Gpsis: gpsiSlice,
			Nssai: &models.Nssai{
				DefaultSingleNssais: snssaiList,
				SingleNssais:        snssaiList,
			},
			// Directly use the pre-calculated value of aggregatedQoS
			SubscribedUeAmbr: &models.AmbrRm{
				// Convert the downlink and uplink bit rates from uint64 to string format required by the model.
				Downlink: convertToString(uint64(aggregatedQoS.DnnMbrDownlink)),
				Uplink:   convertToString(uint64(aggregatedQoS.DnnMbrUplink)),
			},
		}
	} else {
		bsonBytes, errMarshal := bson.Marshal(existingRecord) // Use a different name for error
		if errMarshal != nil {
			logger.DbLog.Errorf("Failed to marshal existing record: %v", errMarshal)
			return
		}
		errUnmarshal := bson.Unmarshal(bsonBytes, &amData) // Use a different name for error
		if errUnmarshal != nil {
			logger.DbLog.Errorf("Failed to unmarshal existing record: %v", errUnmarshal)
			return
		}
		nextSlice := *snssai
		if !containsSnssai(amData.Nssai.SingleNssais, nextSlice) && !containsSnssai(amData.Nssai.DefaultSingleNssais, nextSlice) {
			logger.DbLog.Infof("Appending new S-NSSAI %v to subscriber %s", nextSlice, imsi)
			amData.Nssai.SingleNssais = append(amData.Nssai.SingleNssais, nextSlice)
			amData.Nssai.DefaultSingleNssais = append(amData.Nssai.DefaultSingleNssais, nextSlice)
		}

	}
	// Convert the Go struct `amData` into a BSON map, which is the format required by the MongoDB driver.
	amDataBsonA := configmodels.ToBsonM(amData)
	amDataBsonA["ueId"] = "imsi-" + imsi
	amDataBsonA["servingPlmnId"] = mcc + mnc

	// Create a filter to uniquely identify the document in the database for update or insertion.
	logger.DbLog.Infof("*** Data to be sent to database - AmProvisionedData: %+v", amDataBsonA)
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(amDataColl, filter, amDataBsonA)
	if errPost != nil {
		logger.DbLog.Warnln(errPost)
	}
}

func updateSmProvisionedData(snssai *models.Snssai, dnnMap map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos, mcc, mnc, imsi string) {
	// Define the filter to find the existing record for this UE
	filter := bson.M{
		"ueId":          "imsi-" + imsi,
		"servingPlmnId": mcc + mnc,
	}

	// Fetch the existing record from the database
	existingRecord, err := dbadapter.CommonDBClient.RestfulAPIGetOne(smDataColl, filter)
	if err != nil && err.Error() != "mongo: no documents in result" {
		logger.DbLog.Warnf("Failed to fetch existing record for ueId: %s, error: %v", imsi, err)
		return
	}

	var snssaiList []models.Snssai
	snssaiList = append(snssaiList, *snssai)

	var smData models.SessionManagementSubscriptionData
	if existingRecord == nil {
		// No existing record, create a new one
		smData = models.SessionManagementSubscriptionData{
			SingleNssai:       snssaiList,
			DnnConfigurations: make(map[string]models.DnnConfiguration),
		}
	} else {
		// Convert existing record to struct
		// Convert existing record to BSON properly
		bsonBytes, errMarshal := bson.Marshal(existingRecord) // Use a different name for error
		if errMarshal != nil {
			logger.DbLog.Errorf("Failed to marshal existing record: %v", errMarshal)
			return
		}

		// Unmarshal BSON into struct
		errUnmarshal := bson.Unmarshal(bsonBytes, &smData) // Use a different name for error
		if errUnmarshal != nil {
			logger.DbLog.Errorf("Failed to unmarshal existing record: %v", errUnmarshal)
			return
		}
		nextSlice := *snssai
		if !containsSnssai(smData.SingleNssai, nextSlice) {
			logger.DbLog.Infof("Appending new S-NSSAI %v to subscriber %s", nextSlice, imsi)
			smData.SingleNssai = append(smData.SingleNssai, nextSlice)
		}

	}
	// Iterate over DNNs and add/update their configurations
	for dnn, ueDnnQosList := range dnnMap {
		aggregatedQoS := aggregateQoS(ueDnnQosList) // Combine multiple QoS per DNN

		smData.DnnConfigurations[dnn] = models.DnnConfiguration{
			PduSessionTypes: &models.PduSessionTypes{
				DefaultSessionType:  models.PduSessionType_IPV4,
				AllowedSessionTypes: []models.PduSessionType{models.PduSessionType_IPV4},
			},
			SscModes: &models.SscModes{
				DefaultSscMode: models.SscMode__1,
				AllowedSscModes: []models.SscMode{
					"SSC_MODE_2",
					"SSC_MODE_3",
				},
			},
			SessionAmbr: &models.Ambr{
				Downlink: convertToString(uint64(aggregatedQoS.DnnMbrDownlink)),
				Uplink:   convertToString(uint64(aggregatedQoS.DnnMbrUplink)),
			},
			Var5gQosProfile: &models.SubscribedDefaultQos{
				Var5qi: aggregatedQoS.TrafficClass.Qci,
				Arp: &models.Arp{
					PriorityLevel: 8,
				},
				PriorityLevel: 8,
			},
		}
	}

	// Convert to BSON format
	// Convert smData to BSON format properly

	bsonBytes, err := bson.Marshal(smData)
	if err != nil {
		logger.DbLog.Errorf("Failed to marshal smData: %v", err)
		return
	}

	// Unmarshal BSON into map[string]interface{} for updating MongoDB
	var smDataBsonA map[string]interface{}
	err = bson.Unmarshal(bsonBytes, &smDataBsonA)
	if err != nil {
		logger.DbLog.Errorf("Failed to unmarshal smData BSON: %v", err)
		return
	}

	// Add required fields
	smDataBsonA["ueId"] = "imsi-" + imsi
	smDataBsonA["servingPlmnId"] = mcc + mnc

	// Update the database
	logger.DbLog.Infof("Data to be sent to database - SmProvisionedData: %+v", smDataBsonA)
	// jsonData, _ := json.MarshalIndent(smDataBsonA, "", "  ")
	// logger.DbLog.Infof("Final JSON before MongoDB update: %s", string(jsonData))
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(smDataColl, filter, smDataBsonA)
	if errPost != nil {
		logger.DbLog.Warnln("Failed to update DNN configuration:", errPost)
	}
}
func containsSnssai(list []models.Snssai, target models.Snssai) bool {
	for _, v := range list {
		if v.Sst == target.Sst && v.Sd == target.Sd {
			return true
		}
	}
	return false
}
func aggregateQoS(qosList []configmodels.DeviceGroupsIpDomainExpandedUeDnnQos) configmodels.DeviceGroupsIpDomainExpandedUeDnnQos {
	var aggregated configmodels.DeviceGroupsIpDomainExpandedUeDnnQos
	for _, qos := range qosList {
		aggregated.DnnMbrUplink += qos.DnnMbrUplink
		aggregated.DnnMbrDownlink += qos.DnnMbrDownlink
		aggregated.BitrateUnit = qos.BitrateUnit
		if qos.TrafficClass != nil {
			aggregated.TrafficClass = qos.TrafficClass
		}
	}
	return aggregated
}

func updateSmfSelectionProvisionedData(snssai *models.Snssai, mcc, mnc string, dnnMap map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos, imsi string) {
	// Create the base SmfSelectionSubscriptionData structure
	smfSelData := models.SmfSelectionSubscriptionData{
		SubscribedSnssaiInfos: map[string]models.SnssaiInfo{},
	}

	// Prepare SnssaiInfo for this snssai
	snssaiInfo := models.SnssaiInfo{
		DnnInfos: []models.DnnInfo{},
	}

	// Define the filter for the database operation
	filter := bson.M{
		"ueId":          "imsi-" + imsi,
		"servingPlmnId": mcc + mnc,
	}

	existingRecord, err := dbadapter.CommonDBClient.RestfulAPIGetOne(smfSelDataColl, filter)
	if err != nil && err.Error() != "mongo: no documents in result" {
		logger.DbLog.Warnf("Failed to fetch existing record for ueId: %s, error: %v", imsi, err)
		return
	}

	if existingRecord == nil {
		// Iterate through the dnnMap to populate DnnInfos
		for dnn := range dnnMap {
			// Append each DNN's info to DnnInfos
			snssaiInfo.DnnInfos = append(snssaiInfo.DnnInfos, models.DnnInfo{
				Dnn: dnn,
			})
		}
		// Add the SnssaiInfo to the map using the hex representation of the snssai
		smfSelData.SubscribedSnssaiInfos[SnssaiModelsToHex(*snssai)] = snssaiInfo
	} else {
		if _, exists := smfSelData.SubscribedSnssaiInfos[SnssaiModelsToHex(*snssai)]; exists {
			logger.DbLog.Infof("SNSSAI already exists for UE %s, skipping append.", imsi)
		} else {
			logger.DbLog.Infof("Adding new SNSSAI  entry for UE %s", imsi)
			// Fill DNN list for this SNSSAI
			for dnn := range dnnMap {
				snssaiInfo.DnnInfos = append(snssaiInfo.DnnInfos, models.DnnInfo{
					Dnn: dnn,
				})
			}

			// Insert new entry
			smfSelData.SubscribedSnssaiInfos[SnssaiModelsToHex(*snssai)] = snssaiInfo
		}

	}

	// Convert to BSON format
	smfSelecDataBsonA := configmodels.ToBsonM(smfSelData)
	smfSelecDataBsonA["ueId"] = "imsi-" + imsi
	smfSelecDataBsonA["servingPlmnId"] = mcc + mnc

	// Log the data to be sent to the database
	logger.DbLog.Infof("Data to be sent to database - smf selection: %+v", smfSelecDataBsonA)

	// Perform the database post operation
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(smfSelDataColl, filter, smfSelecDataBsonA)
	if errPost != nil {
		logger.DbLog.Warnln(errPost)
	}
}

func isDeviceGroupExistInSlice(msg *Update5GSubscriberMsg) *configmodels.Slice {
	for name, slice := range getSlices() {
		for _, dgName := range slice.SiteDeviceGroup {
			if dgName == msg.Msg.DevGroupName {
				logger.WebUILog.Infof("device Group [%v] is part of slice: %v", dgName, name)
				return slice
			}
		}
	}

	return nil
}

func getAddedGroupsList(slice, prevSlice *configmodels.Slice) (names []string) {
	return getDeleteGroupsList(prevSlice, slice)
}

func getDeleteGroupsList(slice, prevSlice *configmodels.Slice) (names []string) {
	for prevSlice == nil {
		return
	}

	if slice != nil {
		for _, pdgName := range prevSlice.SiteDeviceGroup {
			var found bool
			for _, dgName := range slice.SiteDeviceGroup {
				if dgName == pdgName {
					found = true
					break
				}
			}
			if !found {
				names = append(names, pdgName)
			}
		}
	} else {
		names = append(names, prevSlice.SiteDeviceGroup...)
	}

	return
}

func removeSubscriberEntriesRelatedToDeviceGroups(mcc, mnc, imsi string) {
	filterImsiOnly := bson.M{"ueId": "imsi-" + imsi}
	filter := bson.M{"ueId": "imsi-" + imsi, "servingPlmnId": mcc + mnc}
	errDelOneAmPol := dbadapter.CommonDBClient.RestfulAPIDeleteOne(amPolicyDataColl, filterImsiOnly)
	if errDelOneAmPol != nil {
		logger.DbLog.Warnln(errDelOneAmPol)
	}
	errDelOneSmPol := dbadapter.CommonDBClient.RestfulAPIDeleteOne(smPolicyDataColl, filterImsiOnly)
	if errDelOneSmPol != nil {
		logger.DbLog.Warnln(errDelOneSmPol)
	}
	errDelOneAmData := dbadapter.CommonDBClient.RestfulAPIDeleteOne(amDataColl, filter)
	if errDelOneAmData != nil {
		logger.DbLog.Warnln(errDelOneAmData)
	}
	errDelOneSmData := dbadapter.CommonDBClient.RestfulAPIDeleteOne(smDataColl, filter)
	if errDelOneSmData != nil {
		logger.DbLog.Warnln(errDelOneSmData)
	}
	errDelOneSmfSel := dbadapter.CommonDBClient.RestfulAPIDeleteOne(smfSelDataColl, filter)
	if errDelOneSmfSel != nil {
		logger.DbLog.Warnln(errDelOneSmfSel)
	}
}

func Config5GUpdateHandle(confChan chan *Update5GSubscriberMsg) {
	for confData := range confChan {
		switch confData.Msg.MsgType {
		case configmodels.Device_group:
			rwLock.RLock()
			/* is this devicegroup part of any existing slice */
			slice := isDeviceGroupExistInSlice(confData)
			if slice != nil {
				sVal, err := strconv.ParseUint(slice.SliceId.Sst, 10, 32)
				if err != nil {
					logger.DbLog.Errorf("could not parse SST %v", slice.SliceId.Sst)
					return
				}
				snssai := &models.Snssai{
					Sd:  slice.SliceId.Sd,
					Sst: int32(sVal),
				}
				/* skip delete case */
				if confData.Msg.MsgMethod != configmodels.Delete_op {
					dnnMap := make(map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos)
					for _, ipDomain := range confData.Msg.DevGroup.IpDomainExpanded {
						if ipDomain.UeDnnQos != nil {
							dnnMap[ipDomain.Dnn] = append(dnnMap[ipDomain.Dnn], *ipDomain.UeDnnQos)
						}
					}

					// Calculate the aggregatedQoS
					var allQosProfiles []configmodels.DeviceGroupsIpDomainExpandedUeDnnQos
					for _, qosList := range dnnMap {
						allQosProfiles = append(allQosProfiles, qosList...)
					}

					aggregatedQoS := aggregateQoS(allQosProfiles)
					for i, imsi := range confData.Msg.DevGroup.Imsis {
						/* update only if the imsi is provisioned */
						if subscriberAuthData.SubscriberAuthenticationDataGet("imsi-"+imsi) != nil {
							var gpsi string
							if confData.Msg.DevGroup.Msisdns != nil && i < len(confData.Msg.DevGroup.Msisdns) {
								gpsi = confData.Msg.DevGroup.Msisdns[i]
							}
							dnnMap := make(map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos)
							for _, ipDomain := range confData.Msg.DevGroup.IpDomainExpanded {
								if ipDomain.UeDnnQos != nil {
									dnnMap[ipDomain.Dnn] = append(dnnMap[ipDomain.Dnn], *ipDomain.UeDnnQos)
								}
							}
							// Call update functions only once per IMSI
							updateSubscriberData(imsi, gpsi, snssai, dnnMap, slice.SiteInfo.Plmn.Mcc, slice.SiteInfo.Plmn.Mnc, aggregatedQoS)
						}
					}
				}
				dimsis := getDeletedImsisList(confData.Msg.DevGroup, confData.PrevDevGroup)
				for _, imsi := range dimsis {
					removeSubscriberEntriesRelatedToDeviceGroups(slice.SiteInfo.Plmn.Mcc, slice.SiteInfo.Plmn.Mnc, imsi)
				}
			}
			rwLock.RUnlock()
		case configmodels.Network_slice:
			rwLock.RLock()
			logger.WebUILog.Debugln("insert/update Network Slice")
			slice := confData.Msg.Slice
			if slice == nil && confData.PrevSlice != nil {
				logger.WebUILog.Debugln("deleted Slice:", confData.PrevSlice)
			}
			if slice != nil {
				sVal, err := strconv.ParseUint(slice.SliceId.Sst, 10, 32)
				if err != nil {
					logger.DbLog.Errorf("could not parse SST %v", slice.SliceId.Sst)
				}
				snssai := &models.Snssai{
					Sd:  slice.SliceId.Sd,
					Sst: int32(sVal),
				}
				mcc := slice.SiteInfo.Plmn.Mcc
				mnc := slice.SiteInfo.Plmn.Mnc
				for _, dgName := range slice.SiteDeviceGroup {
					logger.ConfigLog.Infoln("Processing Device Group:", dgName)

					devGroupConfig := getDeviceGroupByName(dgName)
					if devGroupConfig == nil {
						logger.ConfigLog.Warnln("Device group configuration is nil for dgName:", dgName)
						continue
					}

					if len(devGroupConfig.IpDomainExpanded) == 0 {
						logger.ConfigLog.Warnln("IPDomainExpanded is nil or empty for dgName:", dgName)
						continue
					}

					processDeviceGroup(devGroupConfig, snssai, mcc, mnc)
				}
			}

			dgnames := getDeleteGroupsList(slice, confData.PrevSlice)
			for _, dgname := range dgnames {
				devGroupConfig := getDeviceGroupByName(dgname)
				if devGroupConfig != nil {
					for _, imsi := range devGroupConfig.Imsis {
						removeSubscriberEntriesRelatedToDeviceGroups(confData.PrevSlice.SiteInfo.Plmn.Mcc, confData.PrevSlice.SiteInfo.Plmn.Mnc, imsi)
					}
				}
			}
			rwLock.RUnlock()
		}
	} // end of for loop
}

func convertToString(val uint64) string {
	var mbVal, gbVal, kbVal uint64
	kbVal = val / 1000
	mbVal = val / 1000000
	gbVal = val / 1000000000
	var retStr string
	if gbVal != 0 {
		retStr = strconv.FormatUint(gbVal, 10) + " Gbps"
	} else if mbVal != 0 {
		retStr = strconv.FormatUint(mbVal, 10) + " Mbps"
	} else if kbVal != 0 {
		retStr = strconv.FormatUint(kbVal, 10) + " Kbps"
	} else {
		retStr = strconv.FormatUint(val, 10) + " bps"
	}

	return retStr
}

func SnssaiModelsToHex(snssai models.Snssai) string {
	sst := fmt.Sprintf("%02x", snssai.Sst)
	return sst + snssai.Sd
}

func sendPebbleNotification(key string) error {
	cmd := execCommand("pebble", "notify", key)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("couldn't execute a pebble notify: %w", err)
	}
	logger.ConfigLog.Infoln("custom Pebble notification sent")
	return nil
}

func processDeviceGroup(devGroupConfig *configmodels.DeviceGroups, snssai *models.Snssai, mcc, mnc string) {
	dnnMap := make(map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos) // Stores multiple DNNs & their QoS per IMSI
	// Iterate over each expanded IP domain configuration within the device group.
	for _, ipDomain := range devGroupConfig.IpDomainExpanded {
		dnn := ipDomain.Dnn           // Extract the DNN from the current IP domain.
		if ipDomain.UeDnnQos != nil { // Ensure UeDnnQos is not nil before appending
			dnnMap[dnn] = append(dnnMap[dnn], *ipDomain.UeDnnQos)
		}
	}
	var allQosProfiles []configmodels.DeviceGroupsIpDomainExpandedUeDnnQos // Create a slice to hold all QoS profiles from all DNNs in the device group.
	// Iterate through the dnnMap to collect all QoS profiles into a single slice.
	for _, qosList := range dnnMap {
		allQosProfiles = append(allQosProfiles, qosList...)
	}
	// Calculate aggragate QoS once for the entire group
	aggregatedQoS := aggregateQoS(allQosProfiles)
	// Iterate over each IMSI in the device group configuration.
	for i, imsi := range devGroupConfig.Imsis {
		var gpsi string
		if devGroupConfig.Msisdns != nil && i < len(devGroupConfig.Msisdns) {
			gpsi = devGroupConfig.Msisdns[i]
		}
		// Call update functions once after processing all DNNs
		logger.ConfigLog.Infoln("Processing IMSI:", imsi, "with GPSI:", gpsi)
		updateSubscriberData(imsi, gpsi, snssai, dnnMap, mcc, mnc, aggregatedQoS)
	}
}

func updateSubscriberData(imsi string, gpsi string, snssai *models.Snssai, dnnMap map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos, mcc, mnc string, aggregatedQoS configmodels.DeviceGroupsIpDomainExpandedUeDnnQos) {
	updateAmPolicyData(imsi)
	updateSmPolicyData(snssai, dnnMap, imsi)
	updateSmfSelectionProvisionedData(snssai, mcc, mnc, dnnMap, imsi)
	updateAmProvisionedData(gpsi, snssai, aggregatedQoS, mcc, mnc, imsi) // Pass the pre-calculated aggregatedQoS result and gpsi to the function that needs it.
	updateSmProvisionedData(snssai, dnnMap, mcc, mnc, imsi)
	// Log a confirmation message indicating that all updates for the specified IMSI across all its associated DNNs have been completed.
	logger.ConfigLog.Infoln("Updated IMSI:", imsi, "for all DNNs")
}
