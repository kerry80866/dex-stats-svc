package utils

import (
	"errors"
	"fmt"
	"net"
)

func GetLocalIP() (string, error) {
	var fallbackIP string

	interfaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("get interfaces failed: %w", err)
	}

	for _, iface := range interfaces {
		// 跳过未启用和回环接口
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue // 忽略这张网卡
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue
			}

			if isPrivateIPv4(ip) {
				return ip.String(), nil // 优先返回私有 IP
			}

			if fallbackIP == "" {
				fallbackIP = ip.String() // 记录第一个非私有 IPv4（可作为备用）
			}
		}
	}

	if fallbackIP != "" {
		return fallbackIP, nil
	}
	return "", errors.New("no suitable IP found")
}

func isPrivateIPv4(ip net.IP) bool {
	privateBlocks := []net.IPNet{
		{IP: net.IPv4(10, 0, 0, 0), Mask: net.CIDRMask(8, 32)},
		{IP: net.IPv4(172, 16, 0, 0), Mask: net.CIDRMask(12, 32)},
		{IP: net.IPv4(192, 168, 0, 0), Mask: net.CIDRMask(16, 32)},
	}
	for _, block := range privateBlocks {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}
