// <copyright file="Session.cs" company="Drastic Actions">
// Copyright (c) Drastic Actions. All rights reserved.
// </copyright>

using CarpaNet;
using CarpaNet.Identity;
using System;
using System.Text.Json.Serialization;

namespace AppViewLite;


public class Session
{

    [JsonPropertyName("did")]
    [JsonConverter(typeof(ATDidJsonConverter))]
    public ATDid Did { get; set; }

    [JsonPropertyName("didDoc")]
    public DidDocument? DidDoc { get; set; }

    [JsonPropertyName("handle")]
    public ATHandle Handle { get; set; }

    [JsonPropertyName("email")]
    public string? Email { get; set; }

    [JsonPropertyName("accessJwt")]
    public string AccessJwt { get; set; } = null!;

    [JsonPropertyName("refreshJwt")]
    public string RefreshJwt { get; set; } = null!;

    [JsonPropertyName("expiresIn")]
    [Obsolete] 
    public DateTime ExpiresIn { get; set; }
}
