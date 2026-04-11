package com.buk.hackathon.dto;


import lombok.Data;

@Data
public class CreateTransactionRequest {

    private String userId;
    private Double amount;
}
