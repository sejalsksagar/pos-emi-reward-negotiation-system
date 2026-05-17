package com.pos.dto;


import lombok.Data;

@Data
public class CreateTransactionRequest {

    private String userId;
    private Double amount;
}
