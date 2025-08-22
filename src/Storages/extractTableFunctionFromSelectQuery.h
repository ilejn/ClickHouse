#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{

ASTPtr extractTableFunctionFromSelectQueryPtr(ASTPtr & query);
ASTFunction * extractTableFunctionFromSelectQuery(ASTPtr & query);
ASTExpressionList * extractTableFunctionArgumentsFromSelectQuery(ASTPtr & query);

}
