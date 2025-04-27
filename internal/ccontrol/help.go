/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ccontrol

import (
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"
)

func showHelp() {
	fmt.Printf("Usage: ccontrol [OPTIONS] COMMAND\n\n")
	fmt.Printf("Display and modify the specified entity\n\n")
	fmt.Printf("Options:\n")
	fmt.Printf("  -C, --config string   Path to configuration file (default \"%s\")\n", util.DefaultConfigPath)
	fmt.Printf("      --json            Output in JSON format\n")
	fmt.Printf("  -h, --help            Show this help message\n")
	fmt.Printf("  -v, --version         Show version information\n\n")
	fmt.Printf("Commands:\n")
	fmt.Printf("  show       Display details of the specified entity\n")
	fmt.Printf("  update     Modify attributes of the specified entity\n")
	fmt.Printf("  hold       Prevent specified job from starting\n")
	fmt.Printf("  release    Permit specified job to start\n")
	fmt.Printf("  create     Create a new entity\n")
	fmt.Printf("  delete     Delete an existing entity\n\n")
	fmt.Printf("Use \"ccontrol [command] --help\" for more information about a command.\n")
	os.Exit(0)
}

func showVersion() {
	fmt.Println(util.Version())
	os.Exit(0)
}

func showCommandHelp(command string) {
	switch command {
	case "show":
		showShowCommandHelp()
	case "update":
		showUpdateCommandHelp()
	case "hold":
		showHoldCommandHelp()
	case "release":
		showReleaseCommandHelp()
	case "create":
		showCreateCommandHelp()
	case "delete":
		showDeleteCommandHelp()
	default:
		fmt.Printf("Unknown command: %s\n", command)
		showHelp()
	}
	os.Exit(0)
}

func showShowCommandHelp() {
	fmt.Printf("Usage: ccontrol show RESOURCE [OPTIONS] [ARGS]\n\n")
	fmt.Printf("Display details of the specified entity\n\n")
	fmt.Printf("Resources:\n")
	fmt.Printf("  node [node_name]        Display details of the nodes, default is all\n")
	fmt.Printf("  partition [part_name]   Display details of the partitions, default is all\n")
	fmt.Printf("  job [job_id,...]        Display details of the jobs, default is all\n")
	fmt.Printf("  config                  Display the configuration file\n")
	fmt.Printf("  reservation [res_name]  Display details of the reservations, default is all\n\n")
	fmt.Printf("Global Options:\n")
	fmt.Printf("  -C, --config string   Path to configuration file (default \"%s\")\n", util.DefaultConfigPath)
	fmt.Printf("      --json            Output in JSON format\n")
}

func showUpdateCommandHelp() {
	fmt.Printf("Usage: ccontrol update RESOURCE [OPTIONS] [ARGS]\n\n")
	fmt.Printf("Modify attributes of the specified entity\n\n")
	fmt.Printf("Resources:\n")
	fmt.Printf("  node     Modify node attributes\n")
	fmt.Printf("    Options:\n")
	fmt.Printf("      -n, --name string    Specify names of the node to be modified (comma separated list)\n")
	fmt.Printf("      -t, --state string   Set the node state\n")
	fmt.Printf("      -r, --reason string  Set the reason of this state change\n\n")

	fmt.Printf("  job      Modify job attributes\n")
	fmt.Printf("    Options:\n")
	fmt.Printf("      -J, --job string         Specify job ids of the job to be modified (comma separated list) [required]\n")
	fmt.Printf("      -T, --time-limit string  Set time limit of the job\n")
	fmt.Printf("      -P, --priority float     Set the priority of the job\n\n")

	fmt.Printf("  partition partition_name    Modify partition attributes\n")
	fmt.Printf("    Options:\n")
	fmt.Printf("      -A, --allowed-accounts string  Set the allow account list for the partition\n")
	fmt.Printf("      -D, --denied-accounts string   Set the denied account list for the partition\n")
	fmt.Printf("      Note: Either --allowed-accounts or --denied-accounts must be specified, but not both\n\n")

	fmt.Printf("Global Options:\n")
	fmt.Printf("  -C, --config string   Path to configuration file (default \"%s\")\n", util.DefaultConfigPath)
	fmt.Printf("      --json            Output in JSON format\n")
}

func showHoldCommandHelp() {
	fmt.Printf("Usage: ccontrol hold [OPTIONS] job_id[,job_id...]\n\n")
	fmt.Printf("Prevent specified job from starting\n\n")
	fmt.Printf("Arguments:\n")
	fmt.Printf("  job_id[,job_id...]    Comma-separated list of job IDs to hold\n\n")
	fmt.Printf("Options:\n")
	fmt.Printf("  -t, --time string     Specify the duration the job will be prevented from starting\n\n")
	fmt.Printf("Global Options:\n")
	fmt.Printf("  -C, --config string   Path to configuration file (default \"%s\")\n", util.DefaultConfigPath)
	fmt.Printf("      --json            Output in JSON format\n")
}

func showReleaseCommandHelp() {
	fmt.Printf("Usage: ccontrol release job_id[,job_id...]\n\n")
	fmt.Printf("Permit specified job to start\n\n")
	fmt.Printf("Arguments:\n")
	fmt.Printf("  job_id[,job_id...]    Comma-separated list of job IDs to release\n\n")
	fmt.Printf("Global Options:\n")
	fmt.Printf("  -C, --config string   Path to configuration file (default \"%s\")\n", util.DefaultConfigPath)
	fmt.Printf("      --json            Output in JSON format\n")
}

func showCreateCommandHelp() {
	fmt.Printf("Usage: ccontrol create RESOURCE [OPTIONS]\n\n")
	fmt.Printf("Create a new entity\n\n")
	fmt.Printf("Resources:\n")
	fmt.Printf("  reservation    Create a new reservation\n")
	fmt.Printf("    Options:\n")
	fmt.Printf("      -N, --name string         Name of the reservation [required]\n")
	fmt.Printf("      -S, --start-time string   Start time of the reservation in format YYYY-MM-DD[THH:MM[:SS]] [required]\n")
	fmt.Printf("      -D, --duration string     Duration of the reservation in days-hh:mm:ss format [required]\n")
	fmt.Printf("      -n, --nodes string        Nodes to be reserved (comma separated list) [required]\n")
	fmt.Printf("      -A, --account string      Account allowed to use the reservation\n")
	fmt.Printf("      -u, --user string         User allowed to use the reservation\n\n")
	fmt.Printf("Global Options:\n")
	fmt.Printf("  -C, --config string   Path to configuration file (default \"%s\")\n", util.DefaultConfigPath)
	fmt.Printf("      --json            Output in JSON format\n")
}

func showDeleteCommandHelp() {
	fmt.Printf("Usage: ccontrol delete RESOURCE [ARGS]\n\n")
	fmt.Printf("Delete an existing entity\n\n")
	fmt.Printf("Resources:\n")
	fmt.Printf("  reservation reservation_name   Delete an existing reservation\n")
	fmt.Printf("    Arguments:\n")
	fmt.Printf("      reservation_name    Name of the reservation to delete [required]\n\n")
	fmt.Printf("Global Options:\n")
	fmt.Printf("  -C, --config string   Path to configuration file (default \"%s\")\n", util.DefaultConfigPath)
	fmt.Printf("      --json            Output in JSON format\n")
}

func showSubCommandHelp(command, subCommand string) {
	switch command {
	case "show":
		switch subCommand {
		case "node":
			fmt.Printf("Usage: ccontrol show node [node_name]\n\n")
			fmt.Printf("Display details of the nodes, default is all\n\n")
			fmt.Printf("Arguments:\n")
			fmt.Printf("  node_name    Optional: Name of the node to display\n\n")
			fmt.Printf("Global Options:\n")
			fmt.Printf("  -C, --config string   Path to configuration file (default \"%s\")\n", util.DefaultConfigPath)
			fmt.Printf("      --json            Output in JSON format\n")
		case "partition":
			fmt.Printf("Usage: ccontrol show partition [partition_name]\n\n")
			fmt.Printf("Display details of the partitions, default is all\n\n")
			fmt.Printf("Arguments:\n")
			fmt.Printf("  partition_name    Optional: Name of the partition to display\n\n")
			fmt.Printf("Global Options:\n")
			fmt.Printf("  -C, --config string   Path to configuration file (default \"%s\")\n", util.DefaultConfigPath)
			fmt.Printf("      --json            Output in JSON format\n")
		case "job":
			fmt.Printf("Usage: ccontrol show job [job_id,...]\n\n")
			fmt.Printf("Display details of the jobs, default is all\n\n")
			fmt.Printf("Arguments:\n")
			fmt.Printf("  job_id,...    Optional: Comma-separated list of job IDs to display\n\n")
			fmt.Printf("Global Options:\n")
			fmt.Printf("  -C, --config string   Path to configuration file (default \"%s\")\n", util.DefaultConfigPath)
			fmt.Printf("      --json            Output in JSON format\n")
		case "config":
			fmt.Printf("Usage: ccontrol show config\n\n")
			fmt.Printf("Display the configuration file in key-value format\n\n")
			fmt.Printf("Global Options:\n")
			fmt.Printf("  -C, --config string   Path to configuration file (default \"%s\")\n", util.DefaultConfigPath)
			fmt.Printf("      --json            Output in JSON format\n")
		case "reservation":
			fmt.Printf("Usage: ccontrol show reservation [reservation_name]\n\n")
			fmt.Printf("Display details of the reservations, default is all\n\n")
			fmt.Printf("Arguments:\n")
			fmt.Printf("  reservation_name    Optional: Name of the reservation to display\n\n")
			fmt.Printf("Global Options:\n")
			fmt.Printf("  -C, --config string   Path to configuration file (default \"%s\")\n", util.DefaultConfigPath)
			fmt.Printf("      --json            Output in JSON format\n")
		default:
			showShowCommandHelp()
		}
	case "create":
		switch subCommand {
		case "reservation":
			fmt.Printf("Usage: ccontrol create reservation [OPTIONS]\n\n")
			fmt.Printf("Create a new reservation\n\n")
			fmt.Printf("Options:\n")
			fmt.Printf("  -N, --name string         Name of the reservation [required]\n")
			fmt.Printf("  -S, --start-time string   Start time of the reservation in format YYYY-MM-DD[THH:MM[:SS]] [required]\n")
			fmt.Printf("  -D, --duration string     Duration of the reservation in days-hh:mm:ss format [required]\n")
			fmt.Printf("  -n, --nodes string        Nodes to be reserved (comma separated list) [required]\n")
			fmt.Printf("  -A, --account string      Account allowed to use the reservation\n")
			fmt.Printf("  -u, --user string         User allowed to use the reservation\n\n")
			fmt.Printf("Global Options:\n")
			fmt.Printf("  -C, --config string   Path to configuration file (default \"%s\")\n", util.DefaultConfigPath)
			fmt.Printf("      --json            Output in JSON format\n")
		default:
			showCreateCommandHelp()
		}
	case "delete":
		switch subCommand {
		case "reservation":
			fmt.Printf("Usage: ccontrol delete reservation reservation_name\n\n")
			fmt.Printf("Delete an existing reservation\n\n")
			fmt.Printf("Arguments:\n")
			fmt.Printf("  reservation_name    Name of the reservation to delete [required]\n\n")
			fmt.Printf("Global Options:\n")
			fmt.Printf("  -C, --config string   Path to configuration file (default \"%s\")\n", util.DefaultConfigPath)
			fmt.Printf("      --json            Output in JSON format\n")
		default:
			showDeleteCommandHelp()
		}
	case "update":
		showUpdateCommandHelp()
	case "hold":
		showHoldCommandHelp()
	case "release":
		showReleaseCommandHelp()
	default:
		fmt.Printf("Unknown command: %s %s\n", command, subCommand)
		showHelp()
	}
	os.Exit(0)
}
