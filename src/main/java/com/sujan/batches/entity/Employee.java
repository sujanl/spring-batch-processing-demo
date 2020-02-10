package com.sujan.batches.entity;

import lombok.Data;
import org.springframework.boot.autoconfigure.domain.EntityScan;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Data
public class Employee {
    @Id
    private String employeeId;
    private String firstName;
    private String lastName;
    private int age;
    private String email;
}
