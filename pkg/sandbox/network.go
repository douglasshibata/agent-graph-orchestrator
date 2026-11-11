package sandbox

import (
	"fmt"
	"os/exec"
)

// NetworkConfig specifies network parameters for an ephemeral microVM.
type NetworkConfig struct {
	TapDevice string
	HostIP    string // e.g. "172.28.10.1/30"
	GuestMAC  string
	UID       string // jailer UID, e.g. "10053"
	DryRun    bool   // If true, commands are synthesized without executing system calls
}

// NetworkManager configures host TAP devices and iptables isolation rules.
type NetworkManager struct {
	config NetworkConfig
}

// NewNetworkManager creates a new NetworkManager.
func NewNetworkManager(config NetworkConfig) *NetworkManager {
	if config.UID == "" {
		config.UID = "10053"
	}
	if config.GuestMAC == "" {
		config.GuestMAC = "AA:FC:00:00:00:01"
	}
	return &NetworkManager{config: config}
}

// SetupTap creates and configures the TAP device.
func (m *NetworkManager) SetupTap() error {
	commands := [][]string{
		{"ip", "tuntap", "add", m.config.TapDevice, "mode", "tap", "user", m.config.UID},
		{"ip", "addr", "add", m.config.HostIP, "dev", m.config.TapDevice},
		{"ip", "link", "set", "dev", m.config.TapDevice, "up"},
	}

	for _, args := range commands {
		if m.config.DryRun {
			continue
		}
		cmd := exec.Command(args[0], args[1:]...)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("command %v failed: %w (output: %s)", args, err, string(out))
		}
	}
	return nil
}

// TeardownTap removes the TAP device.
func (m *NetworkManager) TeardownTap() error {
	if m.config.DryRun {
		return nil
	}
	cmd := exec.Command("ip", "link", "del", m.config.TapDevice)
	_ = cmd.Run()
	return nil
}

// GenerateIptablesRules returns the explicit firewall chain rules required to isolate the sandbox.
func (m *NetworkManager) GenerateIptablesRules() []string {
	chain := fmt.Sprintf("SANDBOX_%s", m.config.TapDevice)
	return []string{
		fmt.Sprintf("iptables -N %s", chain),
		fmt.Sprintf("iptables -A FORWARD -i %s -j %s", m.config.TapDevice, chain),
		fmt.Sprintf("iptables -A %s -d 169.254.169.254/32 -j DROP", chain), // Cloud metadata
		fmt.Sprintf("iptables -A %s -d 10.0.0.0/8 -j DROP", chain),         // RFC 1918 Class A
		fmt.Sprintf("iptables -A %s -d 172.16.0.0/12 -j DROP", chain),      // RFC 1918 Class B
		fmt.Sprintf("iptables -A %s -d 192.168.0.0/16 -j DROP", chain),     // RFC 1918 Class C
		fmt.Sprintf("iptables -A %s -m state --state ESTABLISHED,RELATED -j ACCEPT", chain),
		fmt.Sprintf("iptables -A %s -j ACCEPT", chain),                     // Allow public egress or DROP for air-gap
	}
}

// ApplyEgressRules applies the firewall isolation rules using iptables.
func (m *NetworkManager) ApplyEgressRules() error {
	chain := fmt.Sprintf("SANDBOX_%s", m.config.TapDevice)
	ruleSets := [][]string{
		{"iptables", "-N", chain},
		{"iptables", "-A", "FORWARD", "-i", m.config.TapDevice, "-j", chain},
		{"iptables", "-A", chain, "-d", "169.254.169.254/32", "-j", "DROP"},
		{"iptables", "-A", chain, "-d", "10.0.0.0/8", "-j", "DROP"},
		{"iptables", "-A", chain, "-d", "172.16.0.0/12", "-j", "DROP"},
		{"iptables", "-A", chain, "-d", "192.168.0.0/16", "-j", "DROP"},
		{"iptables", "-A", chain, "-m", "state", "--state", "ESTABLISHED,RELATED", "-j", "ACCEPT"},
		{"iptables", "-A", chain, "-j", "ACCEPT"},
	}

	for _, args := range ruleSets {
		if m.config.DryRun {
			continue
		}
		cmd := exec.Command(args[0], args[1:]...)
		_ = cmd.Run()
	}
	return nil
}
