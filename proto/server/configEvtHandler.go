// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0
package server

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/omec-project/openapi/models"
	"github.com/omec-project/webconsole/backend/factory"
	"github.com/omec-project/webconsole/backend/logger"
	"github.com/omec-project/webconsole/configmodels"
	"github.com/omec-project/webconsole/dbadapter"
	"go.mongodb.org/mongo-driver/bson"
	"go.uber.org/zap"
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
	gnbDataColl      = "webconsoleData.snapshots.gnbData"
	upfDataColl      = "webconsoleData.snapshots.upfData"
)

var configLog *zap.SugaredLogger

func init() {
	configLog = logger.ConfigLog
}

type Update5GSubscriberMsg struct {
	Msg          *configmodels.ConfigMessage
	PrevDevGroup *configmodels.DeviceGroups
	PrevSlice    *configmodels.Slice
}

var rwLock sync.RWMutex

var imsiData map[string]*models.AuthenticationSubscription

func init() {
	imsiData = make(map[string]*models.AuthenticationSubscription)
}

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
		configLog.Infoln("Waiting for configuration event ")
		configMsg := <-configMsgChan
		// configLog.Infof("Received configuration event %v ", configMsg)
		if configMsg.MsgType == configmodels.Sub_data {
			imsiVal := strings.ReplaceAll(configMsg.Imsi, "imsi-", "")
			configLog.Infoln("Received imsi from config channel: ", imsiVal)
			rwLock.Lock()
			imsiData[imsiVal] = configMsg.AuthSubData
			rwLock.Unlock()
			configLog.Infof("Received Imsi [%v] configuration from config channel", configMsg.Imsi)
			handleSubscriberPost(configMsg)
			if factory.WebUIConfig.Configuration.Mode5G {
				var configUMsg Update5GSubscriberMsg
				configUMsg.Msg = configMsg
				subsUpdateChan <- &configUMsg
			}
		}

		if configMsg.MsgMethod == configmodels.Post_op || configMsg.MsgMethod == configmodels.Put_op {
			if !firstConfigRcvd && (configMsg.MsgType == configmodels.Device_group || configMsg.MsgType == configmodels.Network_slice) {
				configLog.Debugln("First config received from ROC")
				firstConfigRcvd = true
				configReceived <- true
			}

			// configLog.Infoln("Received msg from configApi package ", configMsg)
			// update config snapshot
			if configMsg.DevGroup != nil {
				configLog.Infof("Received Device Group [%v] configuration from config channel", configMsg.DevGroupName)
				handleDeviceGroupPost(configMsg, subsUpdateChan)
			}

			if configMsg.Slice != nil {
				configLog.Infof("Received Slice [%v] configuration from config channel", configMsg.SliceName)
				handleNetworkSlicePost(configMsg, subsUpdateChan)
			}

			if configMsg.Gnb != nil {
				configLog.Infof("Received gNB [%v] configuration from config channel", configMsg.GnbName)
				handleGnbPost(configMsg)
			}

			if configMsg.Upf != nil {
				configLog.Infof("Received UPF [%v] configuration from config channel", configMsg.UpfHostname)
				handleUpfPost(configMsg)
			}

			// loop through all clients and send this message to all clients
			if len(clientNFPool) == 0 {
				configLog.Infoln("No client available. No need to send config")
			}
			for _, client := range clientNFPool {
				configLog.Infoln("Push config for client : ", client.id)
				client.outStandingPushConfig <- configMsg
			}
		} else {
			var config5gMsg Update5GSubscriberMsg
			if configMsg.MsgType == configmodels.Inventory {
				if configMsg.GnbName != "" {
					configLog.Infof("Received delete gNB [%v] from config channel", configMsg.GnbName)
					handleGnbDelete(configMsg)
				}
				if configMsg.UpfHostname != "" {
					configLog.Infof("Received delete UPF [%v] from config channel", configMsg.UpfHostname)
					handleUpfDelete(configMsg)
				}
			} else if configMsg.MsgType != configmodels.Sub_data {
				rwLock.Lock()
				// update config snapshot
				if configMsg.DevGroup == nil {
					configLog.Infof("Received delete Device Group [%v] from config channel", configMsg.DevGroupName)
					config5gMsg.PrevDevGroup = getDeviceGroupByName(configMsg.DevGroupName)
					filter := bson.M{"group-name": configMsg.DevGroupName}
					errDelOne := dbadapter.CommonDBClient.RestfulAPIDeleteOne(devGroupDataColl, filter)
					if errDelOne != nil {
						logger.DbLog.Warnln(errDelOne)
					}
				}

				if configMsg.Slice == nil {
					configLog.Infof("Received delete Slice [%v] from config channel", configMsg.SliceName)
					config5gMsg.PrevSlice = getSliceByName(configMsg.SliceName)
					filter := bson.M{"SliceName": configMsg.SliceName}
					errDelOne := dbadapter.CommonDBClient.RestfulAPIDeleteOne(sliceDataColl, filter)
					if errDelOne != nil {
						logger.DbLog.Warnln(errDelOne)
					}
				}
				rwLock.Unlock()
			} else {
				configLog.Infof("Received delete Subscriber [%v] from config channel", configMsg.Imsi)
			}
			if factory.WebUIConfig.Configuration.Mode5G {
				config5gMsg.Msg = configMsg
				subsUpdateChan <- &config5gMsg
			}
			// loop through all clients and send this message to all clients
			if len(clientNFPool) == 0 {
				configLog.Infoln("No client available. No need to send config")
			}
			for _, client := range clientNFPool {
				configLog.Infoln("Push config for client : ", client.id)
				client.outStandingPushConfig <- configMsg
			}
		}
	}
}

func handleSubscriberPost(configMsg *configmodels.ConfigMessage) {
	rwLock.Lock()
	basicAmData := map[string]interface{}{
		"ueId": configMsg.Imsi,
	}
	filter := bson.M{"ueId": configMsg.Imsi}
	basicDataBson := toBsonM(basicAmData)
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(amDataColl, filter, basicDataBson)
	if errPost != nil {
		logger.DbLog.Warnln(errPost)
	}
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
	devGroupDataBsonA := toBsonM(configMsg.DevGroup)
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(devGroupDataColl, filter, devGroupDataBsonA)
	if errPost != nil {
		logger.DbLog.Warnln(errPost)
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
	filter := bson.M{"SliceName": configMsg.SliceName}
	sliceDataBsonA := toBsonM(configMsg.Slice)
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(sliceDataColl, filter, sliceDataBsonA)
	if errPost != nil {
		logger.DbLog.Warnln(errPost)
	}
	rwLock.Unlock()
}

func handleGnbPost(configMsg *configmodels.ConfigMessage) {
	rwLock.Lock()
	filter := bson.M{"name": configMsg.GnbName}
	gnbDataBson := toBsonM(configMsg.Gnb)
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(gnbDataColl, filter, gnbDataBson)
	if errPost != nil {
		logger.DbLog.Warnln(errPost)
	}
	rwLock.Unlock()
}

func handleGnbDelete(configMsg *configmodels.ConfigMessage) {
	rwLock.Lock()
	filter := bson.M{"name": configMsg.GnbName}
	errDelOne := dbadapter.CommonDBClient.RestfulAPIDeleteOne(gnbDataColl, filter)
	if errDelOne != nil {
		logger.DbLog.Warnln(errDelOne)
	}
	rwLock.Unlock()
}

func handleUpfPost(configMsg *configmodels.ConfigMessage) {
	rwLock.Lock()
	filter := bson.M{"hostname": configMsg.UpfHostname}
	upfDataBson := toBsonM(configMsg.Upf)
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(upfDataColl, filter, upfDataBson)
	if errPost != nil {
		logger.DbLog.Warnln(errPost)
	}
	rwLock.Unlock()
}

func handleUpfDelete(configMsg *configmodels.ConfigMessage) {
	rwLock.Lock()
	filter := bson.M{"hostname": configMsg.UpfHostname}
	errDelOne := dbadapter.CommonDBClient.RestfulAPIDeleteOne(upfDataColl, filter)
	if errDelOne != nil {
		logger.DbLog.Warnln(errDelOne)
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
		err := json.Unmarshal(mapToByte(rawDevGroup), &devGroupData)
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
	logger.DbLog.Infof("-------- Raw device group data fetched for group-name '%s': %v", name, devGroupDataInterface)
	if errGetOne != nil {
		logger.DbLog.Warnln(errGetOne)
	}
	var devGroupData configmodels.DeviceGroups
	err := json.Unmarshal(mapToByte(devGroupDataInterface), &devGroupData)
	if err != nil {
		logger.DbLog.Errorf("could not unmarshall device group %v", devGroupDataInterface)
	}
	logger.DbLog.Infof("------Successfully unmarshalled device group data for group-name '%s': %+v", name, devGroupData)
	return &devGroupData
}

/*func getDeviceGroupByName(name string) *configmodels.DeviceGroups {
	logger.DbLog.Infof("----- Fetching device group data for group-name: %s", name)
	filter := bson.M{"group-name": name}

	// Fetch data from the database
	devGroupDataInterface, errGetOne := dbadapter.CommonDBClient.RestfulAPIGetOne(devGroupDataColl, filter)
	if errGetOne != nil {
		logger.DbLog.Warnf("--------- Failed to fetch device group data for group-name '%s': %v", name, errGetOne)
		return nil
	}

	if devGroupDataInterface == nil {
		logger.DbLog.Warnf("--------- No device group data found for group-name: %s", name)
		return nil
	}

	// Log the raw data fetched
	logger.DbLog.Infof("-------- Raw device group data fetched for group-name '%s': %v", name, devGroupDataInterface)

	var devGroupData configmodels.DeviceGroups
	err := json.Unmarshal(mapToByte(devGroupDataInterface), &devGroupData)
	if err != nil {
		logger.DbLog.Errorf("Failed to unmarshal device group data for group-name '%s': %v", name, err)
		return nil
	}

	// Log the unmarshalled device group data
	logger.DbLog.Infof("------Successfully unmarshalled device group data for group-name '%s': %+v", name, devGroupData)

	return &devGroupData
}*/

func getSlices() []*configmodels.Slice {
	rawSlices, errGetMany := dbadapter.CommonDBClient.RestfulAPIGetMany(sliceDataColl, nil)
	if errGetMany != nil {
		logger.DbLog.Warnln(errGetMany)
	}
	var slices []*configmodels.Slice
	for _, rawSlice := range rawSlices {
		var sliceData configmodels.Slice
		err := json.Unmarshal(mapToByte(rawSlice), &sliceData)
		if err != nil {
			logger.DbLog.Errorf("could not unmarshall slice %v", rawSlice)
		}
		slices = append(slices, &sliceData)
	}
	return slices
}

func getSliceByName(name string) *configmodels.Slice {
	filter := bson.M{"SliceName": name}
	sliceDataInterface, errGetOne := dbadapter.CommonDBClient.RestfulAPIGetOne(sliceDataColl, filter)
	if errGetOne != nil {
		logger.DbLog.Warnln(errGetOne)
	}
	var sliceData configmodels.Slice
	err := json.Unmarshal(mapToByte(sliceDataInterface), &sliceData)
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
			if imsiData[imsi] != nil {
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
	amPolicy.SubscCats = append(amPolicy.SubscCats, "free5gc")
	amPolicyDatBsonA := toBsonM(amPolicy)
	amPolicyDatBsonA["ueId"] = "imsi-" + imsi
	filter := bson.M{"ueId": "imsi-" + imsi}
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(amPolicyDataColl, filter, amPolicyDatBsonA)
	if errPost != nil {
		logger.DbLog.Warnln(errPost)
	}
}

/*
	func updateSmPolicyData(snssai *models.Snssai, dnnMap map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos, imsi string) {
		var smPolicyData models.SmPolicyData
		var smPolicySnssaiData models.SmPolicySnssaiData
		dnnData := map[string]models.SmPolicyDnnData{
			dnn: {
				Dnn: dnn,
			},
		}
		// smpolicydata
		smPolicySnssaiData.Snssai = snssai
		smPolicySnssaiData.SmPolicyDnnData = dnnData
		smPolicyData.SmPolicySnssaiData = make(map[string]models.SmPolicySnssaiData)
		smPolicyData.SmPolicySnssaiData[SnssaiModelsToHex(*snssai)] = smPolicySnssaiData
		smPolicyDatBsonA := toBsonM(smPolicyData)
		smPolicyDatBsonA["ueId"] = "imsi-" + imsi
		filter := bson.M{"ueId": "imsi-" + imsi}
		logger.DbLog.Infof("*** Data to be sent to database - smPolicyData: %+v", smPolicyDatBsonA)
		_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(smPolicyDataColl, filter, smPolicyDatBsonA)
		if errPost != nil {
			logger.DbLog.Warnln(errPost)
		}
	}
*/
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

	// smPolicySnssaiData
	smPolicySnssaiData.Snssai = snssai
	smPolicySnssaiData.SmPolicyDnnData = dnnData
	smPolicyData.SmPolicySnssaiData = make(map[string]models.SmPolicySnssaiData)
	smPolicyData.SmPolicySnssaiData[SnssaiModelsToHex(*snssai)] = smPolicySnssaiData

	// Convert to BSON for database insertion
	smPolicyDatBsonA := toBsonM(smPolicyData)
	smPolicyDatBsonA["ueId"] = "imsi-" + imsi
	filter := bson.M{"ueId": "imsi-" + imsi}

	logger.DbLog.Infof("*** Data to be sent to database - smPolicyData: %+v", smPolicyDatBsonA)
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(smPolicyDataColl, filter, smPolicyDatBsonA)
	if errPost != nil {
		logger.DbLog.Warnln(errPost)
	}
}

/*func updateSmPolicyData(snssai *models.Snssai, dnn string, imsi string) {
	// Define the filter to fetch the record
	filter := bson.M{"ueId": "imsi-" + imsi}

	// Fetch the existing record
	existingRecord, err := dbadapter.CommonDBClient.RestfulAPIGetOne(smPolicyDataColl, filter)
	if err != nil {
		if err.Error() == "mongo: no documents in result" {
			// No existing record, create a new one
			logger.DbLog.Infof("No existing record for ueId: %s, creating a new one", imsi)
			existingRecord = bson.M{
				"ueId": imsi,
				"smPolicySnssaiData": map[string]interface{}{
					SnssaiModelsToHex(*snssai): map[string]interface{}{
						"snssai":          snssai,
						"smPolicyDnnData": map[string]interface{}{dnn: models.SmPolicyDnnData{Dnn: dnn}},
					},
				},
			}
		} else {
			// Handle unexpected errors
			logger.DbLog.Warnf("Failed to fetch existing record for ueId: %s, error: %v", imsi, err)
			return
		}
	}

	// Prepare the new DNN data
	dnnData := models.SmPolicyDnnData{
		Dnn: dnn,
	}

	// Merge the new DNN into the existing record
	if smPolicySnssaiData, exists := existingRecord["smPolicySnssaiData"].(map[string]interface{}); exists {
		// Ensure that the map is initialized
		if smPolicySnssaiData == nil {
			smPolicySnssaiData = make(map[string]interface{})
			existingRecord["smPolicySnssaiData"] = smPolicySnssaiData // Make sure the map is written back to the record
		}

		snssaiHex := SnssaiModelsToHex(*snssai)
		if snssaiEntry, found := smPolicySnssaiData[snssaiHex].(map[string]interface{}); found {
			// Ensure that smPolicyDnnData is initialized
			if dnnMap, dnnExists := snssaiEntry["smPolicyDnnData"].(map[string]interface{}); dnnExists {
				// Ensure the map is initialized before assigning to it
				if dnnMap == nil {
					dnnMap = make(map[string]interface{})
					snssaiEntry["smPolicyDnnData"] = dnnMap // Make sure the map is written back to the snssaiEntry
				}
				dnnMap[dnn] = dnnData
			} else {
				// If smPolicyDnnData does not exist, create it
				snssaiEntry["smPolicyDnnData"] = map[string]interface{}{
					dnn: dnnData,
				}
			}
		} else {
			// If Snssai entry does not exist, create it
			smPolicySnssaiData[snssaiHex] = map[string]interface{}{
				"snssai":          snssai,
				"smPolicyDnnData": map[string]interface{}{dnn: dnnData},
			}
		}
	} else {
		// If smPolicySnssaiData does not exist, create it
		existingRecord["smPolicySnssaiData"] = map[string]interface{}{
			SnssaiModelsToHex(*snssai): map[string]interface{}{
				"snssai":          snssai,
				"smPolicyDnnData": map[string]interface{}{dnn: dnnData},
			},
		}
	}

	// Update the record back to the database
	logger.DbLog.Infof("*** Data to be sent to database - smPolicyData: %+v", existingRecord)
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(smPolicyDataColl, filter, existingRecord)
	if errPost != nil {
		logger.DbLog.Warnln(errPost)
	}
}
*/
// C-DAC
/* func updateSmPolicyData(snssai *models.Snssai, dnn string, imsi string) {
	filter := bson.M{"ueId": "imsi-" + imsi}

	// Retrieve existing data using RestfulAPIGetOne
	existingData, err := dbadapter.CommonDBClient.RestfulAPIGetOne(smPolicyDataColl, filter)
	var smPolicyData models.SmPolicyData

	if err != nil {
		if err.Error() == "document not found" { // Handle "not found" error specifically
			smPolicyData = models.SmPolicyData{
				SmPolicySnssaiData: make(map[string]models.SmPolicySnssaiData),
			}
		} else {
			logger.DbLog.Warnf("Error retrieving SmPolicyData for imsi-%s: %v", imsi, err)
			return
		}
	} else {
		// Convert retrieved data to SmPolicyData structure
		if err := fromBsonM(existingData, &smPolicyData); err != nil {
			logger.DbLog.Warnf("Error converting existing data for imsi-%s: %v", imsi, err)
			return
		}
	}

	// Convert Snssai to Hex for use as the map key
	snssaiKey := SnssaiModelsToHex(*snssai)

	// Check if the Snssai already exists, otherwise initialize it
	smPolicySnssaiData, exists := smPolicyData.SmPolicySnssaiData[snssaiKey]
	if !exists {
		smPolicySnssaiData = models.SmPolicySnssaiData{
			Snssai:          snssai,
			SmPolicyDnnData: make(map[string]models.SmPolicyDnnData),
		}
	}

	// Add or update the DNN data
	smPolicySnssaiData.SmPolicyDnnData[dnn] = models.SmPolicyDnnData{
		Dnn: dnn,
		// Populate additional fields as needed
	}

	// Update the SmPolicySnssaiData map
	smPolicyData.SmPolicySnssaiData[snssaiKey] = smPolicySnssaiData

	// Convert to BSON and update the document in the database
	smPolicyDatBsonA := toBsonM(smPolicyData)
	smPolicyDatBsonA["ueId"] = "imsi-" + imsi
	logger.DbLog.Infof("*** Data to be sent to database - smPolicyData: %+v", smPolicyDatBsonA)
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(smPolicyDataColl, filter, smPolicyDatBsonA)
	if errPost != nil {
		logger.DbLog.Warnf("Error updating SmPolicyData for imsi-%s: %v", imsi, errPost)
	}
}*/

/*func fromBsonM(input map[string]interface{}, output interface{}) error {
	data, err := bson.Marshal(input)
	if err != nil {
		return err
	}
	return bson.Unmarshal(data, output)
}*/

/*
	func updateAmProvisionedData(snssai *models.Snssai, dnnMap map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos, mcc, mnc, imsi string) {
		amData := models.AccessAndMobilitySubscriptionData{
			Gpsis: []string{
				"msisdn-0900000000",
			},
			Nssai: &models.Nssai{
				DefaultSingleNssais: []models.Snssai{*snssai},
				SingleNssais:        []models.Snssai{*snssai},
			},
			SubscribedUeAmbr: &models.AmbrRm{
				Downlink: convertToString(uint64(qos.DnnMbrDownlink)),
				Uplink:   convertToString(uint64(qos.DnnMbrUplink)),
			},
		}
		amDataBsonA := toBsonM(amData)
		amDataBsonA["ueId"] = "imsi-" + imsi
		amDataBsonA["servingPlmnId"] = mcc + mnc
		filter := bson.M{
			"ueId": "imsi-" + imsi,
			"$or": []bson.M{
				{"servingPlmnId": mcc + mnc},
				{"servingPlmnId": bson.M{"$exists": false}},
			},
		}
		logger.DbLog.Infof("*** Data to be sent to database - AmProvisionedData: %+v", amDataBsonA)
		_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(amDataColl, filter, amDataBsonA)
		if errPost != nil {
			logger.DbLog.Warnln(errPost)
		}
	}
*/
func updateAmProvisionedData(snssai *models.Snssai, dnnMap map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos, mcc, mnc, imsi string) {
	for dnn, ueDnnQosList := range dnnMap {
		aggregatedQoS := aggregateQoS(ueDnnQosList) // Combine multiple QoS into one if needed
		amData := models.AccessAndMobilitySubscriptionData{
			Gpsis: []string{"msisdn-0900000000"},
			Nssai: &models.Nssai{
				DefaultSingleNssais: []models.Snssai{*snssai},
				SingleNssais:        []models.Snssai{*snssai},
			},
			SubscribedUeAmbr: &models.AmbrRm{
				Downlink: convertToString(uint64(aggregatedQoS.DnnMbrDownlink)),
				Uplink:   convertToString(uint64(aggregatedQoS.DnnMbrUplink)),
			},
		}

		amDataBsonA := toBsonM(amData)
		amDataBsonA["ueId"] = "imsi-" + imsi
		amDataBsonA["servingPlmnId"] = mcc + mnc
		amDataBsonA["dnn"] = dnn
		delete(amDataBsonA, "dnn")
		filter := bson.M{
			"ueId":          "imsi-" + imsi,
			"servingPlmnId": mcc + mnc,
		}

		logger.DbLog.Infof("*** Data to be sent to database - AmProvisionedData: %+v", amDataBsonA)
		_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(amDataColl, filter, amDataBsonA)
		if errPost != nil {
			logger.DbLog.Warnln(errPost)
		}
	}
}

/*func updateSmProvisionedData(snssai *models.Snssai, qos *configmodels.DeviceGroupsIpDomainExpandedUeDnnQos, mcc, mnc, dnn, imsi string) {
	// TODO smData
	logger.DbLog.Infof("*** QoS Data Received: %+v", qos)
	logger.DbLog.Infof("*** Qci Data Received: %+v", qos.TrafficClass.Qci)
	logger.DbLog.Infof("*** Data to be sent to database - SmProvisionedData: %+v", dnn)
	smData := models.SessionManagementSubscriptionData{
		SingleNssai: snssai,
		DnnConfigurations: map[string]models.DnnConfiguration{
			dnn: {
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
					Downlink: convertToString(uint64(qos.DnnMbrDownlink)),
					Uplink:   convertToString(uint64(qos.DnnMbrUplink)),
				},
				Var5gQosProfile: &models.SubscribedDefaultQos{
					Var5qi: qos.TrafficClass.Qci,
					Arp: &models.Arp{
						PriorityLevel: 8,
					},
					PriorityLevel: 8,
				},
			},
		},
	}
	// smDataBsonA := toBsonM(smData)
	// smDataBsonA["ueId"] = "imsi-" + imsi
	// smDataBsonA["servingPlmnId"] = mcc + mnc
	// filter := bson.M{"ueId": "imsi-" + imsi, "servingPlmnId": mcc + mnc,}
	smDataBsonA := toBsonM(smData)
	smDataBsonA["ueId"] = "imsi-" + imsi
	smDataBsonA["servingPlmnId"] = mcc + mnc
	smDataBsonA["dnn"] = dnn // Include DNN in the document
	filter := bson.M{
		"ueId":          "imsi-" + imsi,
		"servingPlmnId": mcc + mnc,
		"dnn":           dnn, // Add DNN to the filter
	}
	logger.DbLog.Infof("*** Data to be sent to database - SmProvisionedData: %+v", smDataBsonA)
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(smDataColl, filter, smDataBsonA)
	if errPost != nil {
		logger.DbLog.Warnln(errPost)
	}
} */

/*func updateSmProvisionedData(snssai *models.Snssai, qos *configmodels.DeviceGroupsIpDomainExpandedUeDnnQos, mcc, mnc, dnn, imsi string) {
	logger.DbLog.Infof("*** QoS Data Received: %+v", qos)
	logger.DbLog.Infof("*** Qci Data Received: %+v", qos.TrafficClass.Qci)
	logger.DbLog.Infof("*** Data to be sent to database - SmProvisionedData: %+v", dnn)

	smData := models.SessionManagementSubscriptionData{
		SingleNssai: snssai,
		DnnConfigurations: map[string]models.DnnConfiguration{
			dnn: {
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
					Downlink: convertToString(uint64(qos.DnnMbrDownlink)),
					Uplink:   convertToString(uint64(qos.DnnMbrUplink)),
				},
				Var5gQosProfile: &models.SubscribedDefaultQos{
					Var5qi: qos.TrafficClass.Qci,
					Arp: &models.Arp{
						PriorityLevel: 8,
					},
					PriorityLevel: 8,
				},
			},
		},
	}

	// Convert smData to bson.M
	smDataBsonA := toBsonM(smData)
	smDataBsonA["ueId"] = "imsi-" + imsi
	smDataBsonA["servingPlmnId"] = mcc + mnc
	smDataBsonA["dnn"] = dnn // Include DNN in the document
	logger.DbLog.Infof("SmProvisionedData document: %+v", smDataBsonA)
	// Use filter to find the document based on UE ID and serving PLMN
	filter := bson.M{
		"ueId":          "imsi-" + imsi,
		"servingPlmnId": mcc + mnc,
		"dnn":           dnn, // Add DNN to the filter
	}

	// Log the data that will be sent to the database
	logger.DbLog.Infof("*** Data to be sent to database - SmProvisionedData: %+v", smDataBsonA)

	// Send the data to the database
	err := dbadapter.CommonDBClient.RestfulAPIMergePatch(smDataColl, filter, smDataBsonA)
	if err != nil {
		logger.DbLog.Warnln("Failed to update DNN configuration:", err)
	}
} */

// Updated code
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

	var smData models.SessionManagementSubscriptionData
	if existingRecord == nil {
		// No existing record, create a new one
		smData = models.SessionManagementSubscriptionData{
			SingleNssai:       snssai,
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
	logger.DbLog.Infof("*** Data to be sent to database - SmProvisionedData: %+v", smDataBsonA)
	// jsonData, _ := json.MarshalIndent(smDataBsonA, "", "  ")
	// logger.DbLog.Infof("Final JSON before MongoDB update: %s", string(jsonData))
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(smDataColl, filter, smDataBsonA)
	if errPost != nil {
		logger.DbLog.Warnln("Failed to update DNN configuration:", errPost)
	}
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

	// Iterate through the dnnMap to populate DnnInfos
	for dnn := range dnnMap {
		// Append each DNN's info to DnnInfos
		snssaiInfo.DnnInfos = append(snssaiInfo.DnnInfos, models.DnnInfo{
			Dnn: dnn,
		})
	}

	// Add the SnssaiInfo to the map using the hex representation of the snssai
	smfSelData.SubscribedSnssaiInfos[SnssaiModelsToHex(*snssai)] = snssaiInfo

	// Convert to BSON format
	smfSelecDataBsonA := toBsonM(smfSelData)
	smfSelecDataBsonA["ueId"] = "imsi-" + imsi
	smfSelecDataBsonA["servingPlmnId"] = mcc + mnc

	// Define the filter for the database operation
	filter := bson.M{
		"ueId":          "imsi-" + imsi,
		"servingPlmnId": mcc + mnc,
	}

	// Log the data to be sent to the database
	logger.DbLog.Infof("*** Data to be sent to database - smf selection: %+v", smfSelecDataBsonA)

	// Perform the database post operation
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(smfSelDataColl, filter, smfSelecDataBsonA)
	if errPost != nil {
		logger.DbLog.Warnln(errPost)
	}
}

/*func updateSmfSelectionProviosionedData(snssai *models.Snssai, mcc, mnc, dnn, imsi string) {
	smfSelData := models.SmfSelectionSubscriptionData{
		SubscribedSnssaiInfos: map[string]models.SnssaiInfo{
			SnssaiModelsToHex(*snssai): {
				DnnInfos: []models.DnnInfo{
					{
						Dnn: dnn,
					},
				},
			},
		},
	}
	smfSelecDataBsonA := toBsonM(smfSelData)
	smfSelecDataBsonA["ueId"] = "imsi-" + imsi
	smfSelecDataBsonA["servingPlmnId"] = mcc + mnc
	filter := bson.M{"ueId": "imsi-" + imsi, "servingPlmnId": mcc + mnc}
	logger.DbLog.Infof("*** Data to be sent to database - smf selection: %+v", smfSelecDataBsonA)
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(smfSelDataColl, filter, smfSelecDataBsonA)
	if errPost != nil {
		logger.DbLog.Warnln(errPost)
	}
}*/

/*func updateSmfSelectionProvisionedData(snssai *models.Snssai, mcc, mnc, dnn, imsi string) {
	smfSelData := models.SmfSelectionSubscriptionData{
		SubscribedSnssaiInfos: map[string]models.SnssaiInfo{
			SnssaiModelsToHex(*snssai): {
				DnnInfos: []models.DnnInfo{
					{
						Dnn: dnn,
					},
				},
			},
		},
	}
	smfSelecDataBsonA := toBsonM(smfSelData)
	smfSelecDataBsonA["ueId"] = "imsi-" + imsi
	smfSelecDataBsonA["servingPlmnId"] = mcc + mnc

	filter := bson.M{
		"ueId":          "imsi-" + imsi,
		"servingPlmnId": mcc + mnc,
	}

	// Use $addToSet to add DNN to the existing array (avoids duplicates)
	updateData := bson.M{
		"$addToSet": bson.M{
			"subscribedSnssaiInfos." + SnssaiModelsToHex(*snssai) + ".dnnInfos": bson.M{
				"dnn": dnn,
			},
		},
	}

	logger.DbLog.Infof("*** Data to be sent to database - smf selection: %+v", smfSelecDataBsonA)

	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(smfSelDataColl, filter, updateData)
	if errPost != nil {
		logger.DbLog.Warnln("Failed to update DNN:", errPost)
	}
} */

// C-DAC START
/* func updateSmfSelectionProviosionedData(snssai *models.Snssai, mcc, mnc, dnn, imsi string) {
	// Define the filter
	filter := bson.M{"ueId": "imsi-" + imsi, "servingPlmnId": mcc + mnc}

	// Fetch the existing data
	existingData, errGet := dbadapter.CommonDBClient.RestfulAPIGetOne(smfSelDataColl, filter)
	if errGet != nil {
		logger.DbLog.Warnf("Error fetching SMF selection data for imsi-%s, plmn-%s: %v", imsi, mcc+mnc, errGet)
		existingData = map[string]interface{}{} // Initialize as empty if not found
	}
	// Convert to struct
	var smfSelData models.SmfSelectionSubscriptionData
	err := fromBsonM(existingData, &smfSelData) // Convert BSON to struct
	if err != nil {
		// Handle the error appropriately
		logger.DbLog.Errorf("Error converting BSON to struct:", err)
	}
	// Prepare the new DNN info
	snssaiKey := SnssaiModelsToHex(*snssai)
	if smfSelData.SubscribedSnssaiInfos == nil {
		smfSelData.SubscribedSnssaiInfos = make(map[string]models.SnssaiInfo)
	}
	snssaiInfo, exists := smfSelData.SubscribedSnssaiInfos[snssaiKey]
	if !exists {
		snssaiInfo = models.SnssaiInfo{} // Initialize if not present
	}

	// Update DnnInfos
	snssaiInfo.DnnInfos = append(snssaiInfo.DnnInfos, models.DnnInfo{Dnn: dnn})

	// Assign back to the map
	smfSelData.SubscribedSnssaiInfos[snssaiKey] = snssaiInfo

	// Convert back to BSON and update
	smfSelecDataBsonA := toBsonM(smfSelData)
	smfSelecDataBsonA["ueId"] = "imsi-" + imsi
	smfSelecDataBsonA["servingPlmnId"] = mcc + mnc
	logger.DbLog.Infof("*** Data to be sent to database - smf selection: %+v", smfSelecDataBsonA)
	_, errPost := dbadapter.CommonDBClient.RestfulAPIPost(smfSelDataColl, filter, smfSelecDataBsonA)
	if errPost != nil {
		logger.DbLog.Warnf("Error posting SMF selection data for imsi-%s, plmn-%s: %v", imsi, mcc+mnc, errPost)
	}
} */

// C-DAC END

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

func Config5GUpdateHandle(confChan chan *Update5GSubscriberMsg) {
	for confData := range confChan {
		switch confData.Msg.MsgType {
		case configmodels.Sub_data:
			rwLock.RLock()
			// check this Imsi is part of any of the devicegroup
			imsi := strings.ReplaceAll(confData.Msg.Imsi, "imsi-", "")
			if confData.Msg.MsgMethod != configmodels.Delete_op {
				logger.WebUILog.Debugln("insert/update AuthenticationSubscription ", imsi)
				logger.WebUILog.Infoln("insert/update AuthenticationSubscription ", imsi)
				filter := bson.M{"ueId": confData.Msg.Imsi}
				authDataBsonA := toBsonM(confData.Msg.AuthSubData)
				authDataBsonA["ueId"] = confData.Msg.Imsi
				_, errPost := dbadapter.AuthDBClient.RestfulAPIPost(authSubsDataColl, filter, authDataBsonA)
				if errPost != nil {
					logger.DbLog.Warnln(errPost)
				}
			} else {
				logger.WebUILog.Debugln("delete AuthenticationSubscription", imsi)
				filter := bson.M{"ueId": "imsi-" + imsi}
				errDelOne := dbadapter.AuthDBClient.RestfulAPIDeleteOne(authSubsDataColl, filter)
				if errDelOne != nil {
					logger.DbLog.Warnln(errDelOne)
				}
				errDel := dbadapter.CommonDBClient.RestfulAPIDeleteOne(amDataColl, filter)
				if errDel != nil {
					logger.DbLog.Warnln(errDel)
				}
			}
			rwLock.RUnlock()

		case configmodels.Device_group:
			rwLock.RLock()
			/* is this devicegroup part of any existing slice */
			slice := isDeviceGroupExistInSlice(confData)
			if slice != nil {
				sVal, err := strconv.ParseUint(slice.SliceId.Sst, 10, 32)
				if err != nil {
					logger.DbLog.Errorf("could not parse SST %v", slice.SliceId.Sst)
				}
				snssai := &models.Snssai{
					Sd:  slice.SliceId.Sd,
					Sst: int32(sVal),
				}

				aimsis := getAddedImsisList(confData.Msg.DevGroup, confData.PrevDevGroup)
				if len(aimsis) == 0 {
					logger.DbLog.Warnln("No IMSIs to process")
					return
				}
				for _, imsi := range aimsis {
					// Check if IpDomainExpanded is available
					if len(confData.Msg.DevGroup.IpDomainExpanded) == 0 {
						configLog.Warnln("No IP Domain data available for IMSI:", imsi)
						continue
					}
					configLog.Infoln("Processing IMSI:", imsi)
					// Collect all DNNs and QoS mappings for the IMSI
					dnnMap := make(map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos)
					for _, ipDomain := range confData.Msg.DevGroup.IpDomainExpanded {
						if ipDomain.UeDnnQos != nil {
							dnnMap[ipDomain.Dnn] = append(dnnMap[ipDomain.Dnn], *ipDomain.UeDnnQos)
						}
					}
					// Call update functions only once per IMSI
					updateSubscriberData(imsi, snssai, dnnMap, slice.SiteInfo.Plmn.Mcc, slice.SiteInfo.Plmn.Mnc)
				}

				dimsis := getDeletedImsisList(confData.Msg.DevGroup, confData.PrevDevGroup)
				for _, imsi := range dimsis {
					mcc := slice.SiteInfo.Plmn.Mcc
					mnc := slice.SiteInfo.Plmn.Mnc
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
				/*for _, dgName := range slice.SiteDeviceGroup {
					configLog.Infoln("dgName : ", dgName)
					devGroupConfig := getDeviceGroupByName(dgName)
					if devGroupConfig == nil {
						configLog.Warnln("Device group configuration is nil for dgName:", dgName)
						continue // Skip processing for this device group
					} else {
						configLog.Infoln("Device group configuration not nil for dgName:", dgName)
						configLog.Infoln("Device group configuration details:", devGroupConfig)
						for _, imsi := range devGroupConfig.Imsis {
							if devGroupConfig.IpDomainExpanded != nil {
								configLog.Infoln("confData.Msg.DevGroup is not nil")
								configLog.Infoln("Processing IMSI:", imsi)
								if len(devGroupConfig.IpDomainExpanded) > 0 {
									configLog.Infoln("IPDomainExpanded is not empty")
									for _, ipDomain := range devGroupConfig.IpDomainExpanded {
										configLog.Infoln("Processing IP Domain:", fmt.Sprintf("%+v", ipDomain))
										dnn := ipDomain.Dnn // C-DAC
										mcc := slice.SiteInfo.Plmn.Mcc
										mnc := slice.SiteInfo.Plmn.Mnc
										updateAmPolicyData(imsi)
										updateSmPolicyData(snssai, dnn, imsi)
										updateAmProvisionedData(snssai, ipDomain.UeDnnQos, mcc, mnc, imsi)
										updateSmProvisionedData(snssai, ipDomain.UeDnnQos, mcc, mnc, dnn, imsi)
										updateSmfSelectionProvisionedData(snssai, mcc, mnc, dnn, imsi)
									}
								} else {
									configLog.Warnln("IPDomainExpanded is empty")
								}
							} else {
								configLog.Warnln("confData.Msg.DevGroup is nil")
							}
						}
					}
				} */
				for _, dgName := range slice.SiteDeviceGroup {
					configLog.Infoln("Processing Device Group:", dgName)

					devGroupConfig := getDeviceGroupByName(dgName)
					if devGroupConfig == nil {
						configLog.Warnln("Device group configuration is nil for dgName:", dgName)
						continue
					}

					if len(devGroupConfig.IpDomainExpanded) == 0 {
						configLog.Warnln("IPDomainExpanded is nil or empty for dgName:", dgName)
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
						mcc := confData.PrevSlice.SiteInfo.Plmn.Mcc
						mnc := confData.PrevSlice.SiteInfo.Plmn.Mnc
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
				}
			}
			rwLock.RUnlock()
		}
	} // end of for loop
}

func processDeviceGroup(devGroupConfig *configmodels.DeviceGroups, snssai *models.Snssai, mcc, mnc string) {
	dnnMap := make(map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos) // Stores multiple DNNs & their QoS per IMSI

	for _, imsi := range devGroupConfig.Imsis {
		configLog.Infoln("Processing IMSI:", imsi)

		for _, ipDomain := range devGroupConfig.IpDomainExpanded {
			dnn := ipDomain.Dnn

			// Ensure UeDnnQos is not nil before appending
			if ipDomain.UeDnnQos != nil {
				dnnMap[dnn] = append(dnnMap[dnn], *ipDomain.UeDnnQos) // Directly append the UeDnnQos
			}
		}

		// Call update functions once after processing all DNNs
		updateSubscriberData(imsi, snssai, dnnMap, mcc, mnc)
	}
}

func updateSubscriberData(imsi string, snssai *models.Snssai, dnnMap map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos, mcc, mnc string) {
	updateAmPolicyData(imsi)

	// Pass dnnMap directly to functions that support multiple DNNs
	updateSmPolicyData(snssai, dnnMap, imsi)
	updateSmfSelectionProvisionedData(snssai, mcc, mnc, dnnMap, imsi)
	updateAmProvisionedData(snssai, dnnMap, mcc, mnc, imsi)
	updateSmProvisionedData(snssai, dnnMap, mcc, mnc, imsi) // Updated function to handle multiple DNNs

	configLog.Infoln("Updated IMSI:", imsi, "for all DNNs")
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

// seems something which we should move to mongolib
func toBsonM(data interface{}) (ret bson.M) {
	tmp, err := json.Marshal(data)
	if err != nil {
		logger.DbLog.Errorln("could not marshall data")
		return nil
	}
	err = json.Unmarshal(tmp, &ret)
	if err != nil {
		logger.DbLog.Errorln("could not unmarshall data")
		return nil
	}
	return ret
}

func mapToByte(data map[string]interface{}) (ret []byte) {
	ret, err := json.Marshal(data)
	if err != nil {
		logger.DbLog.Errorln("could not marshall data")
		return nil
	}
	return ret
}

func SnssaiModelsToHex(snssai models.Snssai) string {
	sst := fmt.Sprintf("%02x", snssai.Sst)
	return sst + snssai.Sd
}
