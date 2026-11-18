//go:build !dur053_k6

package k6ingress

import "crypto/tls"

func (r *Relay) handleCampaign(*tls.Conn) bool { return false }

func validCampaignRecord(r Record) bool { return r.Copy == 0 }

func validCampaignEvent(Record) bool { return false }
