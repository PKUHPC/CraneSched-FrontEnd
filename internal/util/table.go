package util

import (
	"strings"

	"github.com/olekukonko/tablewriter"
)

func SetBorderlessTable(table *tablewriter.Table) {
	table.SetBorder(false)
	table.SetAutoFormatHeaders(true)
	table.SetAutoWrapText(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetHeaderLine(false)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetTablePadding(" ")
	table.SetNoWhiteSpace(true)
}

func SetBorderTable(table *tablewriter.Table) {
	table.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: true})
	table.SetAutoFormatHeaders(true)
	table.SetAutoWrapText(true)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("|")
	table.SetTablePadding("\t")
}

func FormatTable(tableOutputWidth []int, tableHeader []string,
	tableData [][]string) (formatTableHeader []string, formatTableData [][]string) {
	for i, h := range tableHeader {
		if tableOutputWidth[i] != -1 {
			padLength := tableOutputWidth[i] - len(h)
			if padLength >= 0 {
				tableHeader[i] = h + strings.Repeat(" ", padLength)
			} else {
				tableHeader[i] = h[:tableOutputWidth[i]]
			}
		}
	}
	for i, row := range tableData {
		for j, cell := range row {
			if tableOutputWidth[j] != -1 {
				padLength := tableOutputWidth[j] - len(cell)
				if padLength >= 0 {
					tableData[i][j] = cell + strings.Repeat(" ", padLength)
				} else {
					tableData[i][j] = cell[:tableOutputWidth[j]]
				}
			}
		}
	}
	return tableHeader, tableData
}

// Trim the cell of the table, if the cell is longer than 32 characters,
// the rest of the characters will be replaced by `...`.
func TrimTable(rows *[][]string) {
	for i, row := range *rows {
		for j, cell := range row {
			if len(cell) > 30 {
				(*rows)[i][j] = cell[:30] + "..."
			}
		}
	}
}

// Trim the cell of the table, if the cell is longer than 32 characters,
// the rest of the characters will be replaced by `...`.
// partNum means do not trim columns
func TrimPartTable(rows *[][]string, partNum int) {
	for i, row := range *rows {
		for j, cell := range row {
			if j == partNum {
				continue
			}
			if len(cell) > 30 {
				(*rows)[i][j] = cell[:30] + "..."
			}
		}
	}
}
