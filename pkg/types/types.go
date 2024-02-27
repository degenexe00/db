package types

import (
	"github.com/MichalPitr/db_from_scratch/pkg/constants"
)

type statementType int

const (
	StmtInsert statementType = iota
	StmtSelect
	StmtDelete
)

type NodeType uint8

const (
	NodeInternal NodeType = iota
	NodeLeaf
)

type Statement struct {
	StmtType    statementType
	RowToInsert Row
	RowToDelete uint32
}

type Row struct {
	Id       uint32
	Username [constants.UsernameSize]byte
	Email    [constants.EmailSize]byte
}

type Page [constants.PageSize]byte
