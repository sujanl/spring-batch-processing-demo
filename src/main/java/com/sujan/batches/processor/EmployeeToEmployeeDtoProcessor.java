package com.sujan.batches.processor;

import com.sujan.batches.dto.EmployeeDto;
import com.sujan.batches.entity.Employee;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class EmployeeToEmployeeDtoProcessor implements ItemProcessor<Employee, EmployeeDto> {

    @Override
    public EmployeeDto process(Employee employee) throws Exception {
        EmployeeDto employeeDTO = new EmployeeDto();
        employeeDTO.setEmployeeId(employee.getEmployeeId());
        employeeDTO.setFirstName(employee.getFirstName());
        employeeDTO.setLastName(employee.getLastName());
        employeeDTO.setEmail(employee.getEmail());
        employeeDTO.setAge(employee.getAge());
//        System.out.println("inside processor " + employee.toString());
        return employeeDTO;
    }
}