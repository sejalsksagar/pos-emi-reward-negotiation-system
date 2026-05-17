package com.pos.entity;


import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Offer {

    private String offerId;
    private String type; // EMI, CASHBACK, REWARD
    private String description;
}
