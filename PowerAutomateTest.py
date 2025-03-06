from pypowerautomate.flow import Flow
from pypowerautomate.triggers import ManualTrigger
from pypowerautomate.actions import InitVariableAction,IncrementVariableAction,VariableTypes
from pypowerautomate.package import Package

flow = Flow()

flow.set_trigger(ManualTrigger("Button"))

flow.add_top_action(InitVariableAction("action1","a",VariableTypes.integer,1))
flow.append_action(IncrementVariableAction("action2","a",2))

package = Package("incremental_test",flow)
package.export_zipfile()