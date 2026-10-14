// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0
//

package configmodels

type SubsListIE struct {
	PlmnID               string `json:"plmnID"`
	UeId                 string `json:"ueId"`
	Msisdn               string `json:"msisdn"`
	Opc                  string `json:"opc"`
	Key                  string `json:"key"`
	AuthenticationMethod string `json:"authenticationMethod"`
	SequenceNumber       string `json:"sequenceNumber"`
}
