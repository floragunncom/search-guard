/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.searchguard.enterprise.femt.datamigration880.service.steps;

interface TestMappings {
    String SIMPLE = """
            {
            	"properties": {
            		"description": {
            			"type": "text",
            			"fields": {
            				"keyword_sub_field": {
            					"type": "keyword",
            					"ignore_above": 215
            				}
            			}
            		}
            	}
            }
            """;
    String MEDIUM = """
            {
                "properties": {
                    "stages": {
                        "properties": {
                            "message": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            },
                            "name": {
                                "type": "keyword"
                            },
                            "start_time": {
                                "type": "date"
                            },
                            "status": {
                                "type": "keyword"
                            },
                            "step_number": {
                                "type": "long"
                            }
                        }
                    },
                    "start_time": {
                        "type": "date"
                    },
                    "status": {
                        "type": "keyword"
                    },
                    "temp_index_name": {
                        "type": "keyword"
                    },
                    "backup_index_name": {
                        "type": "keyword"
                    }
                }
            }
            """;
    String HARD = """
        {
        	"dynamic": "true",
        	"properties": {
        		"action": {
        			"properties": {
        				"actionTypeIds": {
        					"type": "keyword"
        				},
        				"config": {
        					"type": "object",
        					"enabled": false
        				},
        				"isMissingSecrets": {
        					"type": "boolean"
        				},
        				"name": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword"
        						}
        					}
        				},
        				"secrets": {
        					"type": "binary"
        				}
        			}
        		},
        		"action_task_param": {
        			"properties": {
        				"actionId": {
        					"type": "keyword"
        				},
        				"apiKey": {
        					"type": "binary"
        				},
        				"consumer": {
        					"type": "keyword"
        				},
        				"executionId": {
        					"type": "keyword"
        				},
        				"params": {
        					"type": "object",
        					"enabled": false
        				},
        				"relatedSavedObject": {
        					"type": "object",
        					"enabled": false
        				}
        			}
        		},
        		"alerts": {
        			"properties": {
        				"actions": {
        					"type": "nested",
        					"properties": {
        						"actionRef": {
        							"type": "keyword"
        						},
        						"actionTypeId": {
        							"type": "keyword"
        						},
        						"frequency": {
        							"properties": {
        								"notifyWhen": {
        									"type": "keyword",
        									"index": false
        								},
        								"summary": {
        									"type": "boolean",
        									"index": false
        								},
        								"throttle": {
        									"type": "keyword",
        									"index": false
        								}
        							}
        						},
        						"group": {
        							"type": "keyword"
        						},
        						"params": {
        							"type": "object",
        							"enabled": false
        						}
        					}
        				},
        				"alertTypeId": {
        					"type": "keyword"
        				},
        				"apiKey": {
        					"type": "binary"
        				},
        				"apiKeyOwner": {
        					"type": "keyword"
        				},
        				"consumer": {
        					"type": "keyword"
        				},
        				"createdAt": {
        					"type": "date"
        				},
        				"createdBy": {
        					"type": "keyword"
        				},
        				"enabled": {
        					"type": "boolean"
        				},
        				"executionStatus": {
        					"properties": {
        						"error": {
        							"properties": {
        								"message": {
        									"type": "keyword"
        								},
        								"reason": {
        									"type": "keyword"
        								}
        							}
        						},
        						"lastDuration": {
        							"type": "long"
        						},
        						"lastExecutionDate": {
        							"type": "date"
        						},
        						"numberOfTriggeredActions": {
        							"type": "long"
        						},
        						"status": {
        							"type": "keyword"
        						},
        						"warning": {
        							"properties": {
        								"message": {
        									"type": "keyword"
        								},
        								"reason": {
        									"type": "keyword"
        								}
        							}
        						}
        					}
        				},
        				"lastRun": {
        					"properties": {
        						"alertsCount": {
        							"properties": {
        								"active": {
        									"type": "float"
        								},
        								"ignored": {
        									"type": "float"
        								},
        								"new": {
        									"type": "float"
        								},
        								"recovered": {
        									"type": "float"
        								}
        							}
        						},
        						"outcome": {
        							"type": "keyword"
        						},
        						"outcomeMsg": {
        							"type": "text"
        						},
        						"outcomeOrder": {
        							"type": "float"
        						},
        						"warning": {
        							"type": "text"
        						}
        					}
        				},
        				"legacyId": {
        					"type": "keyword"
        				},
        				"mapped_params": {
        					"properties": {
        						"risk_score": {
        							"type": "float"
        						},
        						"severity": {
        							"type": "keyword"
        						}
        					}
        				},
        				"meta": {
        					"properties": {
        						"versionApiKeyLastmodified": {
        							"type": "keyword"
        						}
        					}
        				},
        				"monitoring": {
        					"properties": {
        						"run": {
        							"properties": {
        								"calculated_metrics": {
        									"properties": {
        										"p50": {
        											"type": "long"
        										},
        										"p95": {
        											"type": "long"
        										},
        										"p99": {
        											"type": "long"
        										},
        										"success_ratio": {
        											"type": "float"
        										}
        									}
        								},
        								"history": {
        									"properties": {
        										"duration": {
        											"type": "long"
        										},
        										"outcome": {
        											"type": "keyword"
        										},
        										"success": {
        											"type": "boolean"
        										},
        										"timestamp": {
        											"type": "date"
        										}
        									}
        								},
        								"last_run": {
        									"properties": {
        										"metrics": {
        											"properties": {
        												"duration": {
        													"type": "long"
        												},
        												"gap_duration_s": {
        													"type": "float"
        												},
        												"total_alerts_created": {
        													"type": "float"
        												},
        												"total_alerts_detected": {
        													"type": "float"
        												},
        												"total_indexing_duration_ms": {
        													"type": "long"
        												},
        												"total_search_duration_ms": {
        													"type": "long"
        												}
        											}
        										},
        										"timestamp": {
        											"type": "date"
        										}
        									}
        								}
        							}
        						}
        					}
        				},
        				"muteAll": {
        					"type": "boolean"
        				},
        				"mutedInstanceIds": {
        					"type": "keyword"
        				},
        				"name": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"normalizer": "lowercase"
        						}
        					}
        				},
        				"nextRun": {
        					"type": "date"
        				},
        				"notifyWhen": {
        					"type": "keyword"
        				},
        				"params": {
        					"type": "flattened",
        					"ignore_above": 4096
        				},
        				"running": {
        					"type": "boolean"
        				},
        				"schedule": {
        					"properties": {
        						"interval": {
        							"type": "keyword"
        						}
        					}
        				},
        				"scheduledTaskId": {
        					"type": "keyword"
        				},
        				"snoozeSchedule": {
        					"type": "nested",
        					"properties": {
        						"duration": {
        							"type": "long"
        						},
        						"id": {
        							"type": "keyword"
        						},
        						"rRule": {
        							"type": "nested",
        							"properties": {
        								"byhour": {
        									"type": "long"
        								},
        								"byminute": {
        									"type": "long"
        								},
        								"bymonth": {
        									"type": "short"
        								},
        								"bymonthday": {
        									"type": "short"
        								},
        								"bysecond": {
        									"type": "long"
        								},
        								"bysetpos": {
        									"type": "long"
        								},
        								"byweekday": {
        									"type": "keyword"
        								},
        								"byweekno": {
        									"type": "short"
        								},
        								"byyearday": {
        									"type": "short"
        								},
        								"count": {
        									"type": "long"
        								},
        								"dtstart": {
        									"type": "date",
        									"format": "strict_date_time"
        								},
        								"freq": {
        									"type": "keyword"
        								},
        								"interval": {
        									"type": "long"
        								},
        								"tzid": {
        									"type": "keyword"
        								},
        								"until": {
        									"type": "date",
        									"format": "strict_date_time"
        								},
        								"wkst": {
        									"type": "keyword"
        								}
        							}
        						},
        						"skipRecurrences": {
        							"type": "date",
        							"format": "strict_date_time"
        						}
        					}
        				},
        				"tags": {
        					"type": "keyword"
        				},
        				"throttle": {
        					"type": "keyword"
        				},
        				"updatedAt": {
        					"type": "date"
        				},
        				"updatedBy": {
        					"type": "keyword"
        				}
        			}
        		},
        		"api_key_pending_invalidation": {
        			"properties": {
        				"apiKeyId": {
        					"type": "keyword"
        				},
        				"createdAt": {
        					"type": "date"
        				}
        			}
        		},
        		"apm-indices": {
        			"type": "object",
        			"dynamic": "false"
        		},
        		"apm-server-schema": {
        			"properties": {
        				"schemaJson": {
        					"type": "text",
        					"index": false
        				}
        			}
        		},
        		"apm-service-group": {
        			"properties": {
        				"color": {
        					"type": "text"
        				},
        				"description": {
        					"type": "text"
        				},
        				"groupName": {
        					"type": "keyword"
        				},
        				"kuery": {
        					"type": "text"
        				}
        			}
        		},
        		"apm-telemetry": {
        			"type": "object",
        			"dynamic": "false"
        		},
        		"app_search_telemetry": {
        			"type": "object",
        			"dynamic": "false"
        		},
        		"application_usage_daily": {
        			"dynamic": "false",
        			"properties": {
        				"timestamp": {
        					"type": "date"
        				}
        			}
        		},
        		"application_usage_totals": {
        			"type": "object",
        			"dynamic": "false"
        		},
        		"canvas-element": {
        			"dynamic": "false",
        			"properties": {
        				"@created": {
        					"type": "date"
        				},
        				"@timestamp": {
        					"type": "date"
        				},
        				"content": {
        					"type": "text"
        				},
        				"help": {
        					"type": "text"
        				},
        				"image": {
        					"type": "text"
        				},
        				"name": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword"
        						}
        					}
        				}
        			}
        		},
        		"canvas-workpad": {
        			"dynamic": "false",
        			"properties": {
        				"@created": {
        					"type": "date"
        				},
        				"@timestamp": {
        					"type": "date"
        				},
        				"name": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword"
        						}
        					}
        				}
        			}
        		},
        		"canvas-workpad-template": {
        			"dynamic": "false",
        			"properties": {
        				"help": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword"
        						}
        					}
        				},
        				"name": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword"
        						}
        					}
        				},
        				"tags": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword"
        						}
        					}
        				},
        				"template_key": {
        					"type": "keyword"
        				}
        			}
        		},
        		"cases": {
        			"properties": {
        				"assignees": {
        					"properties": {
        						"uid": {
        							"type": "keyword"
        						}
        					}
        				},
        				"closed_at": {
        					"type": "date"
        				},
        				"closed_by": {
        					"properties": {
        						"email": {
        							"type": "keyword"
        						},
        						"full_name": {
        							"type": "keyword"
        						},
        						"profile_uid": {
        							"type": "keyword"
        						},
        						"username": {
        							"type": "keyword"
        						}
        					}
        				},
        				"connector": {
        					"properties": {
        						"fields": {
        							"properties": {
        								"key": {
        									"type": "text"
        								},
        								"value": {
        									"type": "text"
        								}
        							}
        						},
        						"name": {
        							"type": "text"
        						},
        						"type": {
        							"type": "keyword"
        						}
        					}
        				},
        				"created_at": {
        					"type": "date"
        				},
        				"created_by": {
        					"properties": {
        						"email": {
        							"type": "keyword"
        						},
        						"full_name": {
        							"type": "keyword"
        						},
        						"profile_uid": {
        							"type": "keyword"
        						},
        						"username": {
        							"type": "keyword"
        						}
        					}
        				},
        				"description": {
        					"type": "text"
        				},
        				"external_service": {
        					"properties": {
        						"connector_name": {
        							"type": "keyword"
        						},
        						"external_id": {
        							"type": "keyword"
        						},
        						"external_title": {
        							"type": "text"
        						},
        						"external_url": {
        							"type": "text"
        						},
        						"pushed_at": {
        							"type": "date"
        						},
        						"pushed_by": {
        							"properties": {
        								"email": {
        									"type": "keyword"
        								},
        								"full_name": {
        									"type": "keyword"
        								},
        								"profile_uid": {
        									"type": "keyword"
        								},
        								"username": {
        									"type": "keyword"
        								}
        							}
        						}
        					}
        				},
        				"owner": {
        					"type": "keyword"
        				},
        				"settings": {
        					"properties": {
        						"syncAlerts": {
        							"type": "boolean"
        						}
        					}
        				},
        				"severity": {
        					"type": "short"
        				},
        				"status": {
        					"type": "short"
        				},
        				"tags": {
        					"type": "keyword"
        				},
        				"title": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword"
        						}
        					}
        				},
        				"total_alerts": {
        					"type": "integer"
        				},
        				"total_comments": {
        					"type": "integer"
        				},
        				"updated_at": {
        					"type": "date"
        				},
        				"updated_by": {
        					"properties": {
        						"email": {
        							"type": "keyword"
        						},
        						"full_name": {
        							"type": "keyword"
        						},
        						"profile_uid": {
        							"type": "keyword"
        						},
        						"username": {
        							"type": "keyword"
        						}
        					}
        				}
        			}
        		},
        		"cases-comments": {
        			"properties": {
        				"actions": {
        					"properties": {
        						"targets": {
        							"type": "nested",
        							"properties": {
        								"endpointId": {
        									"type": "keyword"
        								},
        								"hostname": {
        									"type": "keyword"
        								}
        							}
        						},
        						"type": {
        							"type": "keyword"
        						}
        					}
        				},
        				"alertId": {
        					"type": "keyword"
        				},
        				"comment": {
        					"type": "text"
        				},
        				"created_at": {
        					"type": "date"
        				},
        				"created_by": {
        					"properties": {
        						"email": {
        							"type": "keyword"
        						},
        						"full_name": {
        							"type": "keyword"
        						},
        						"profile_uid": {
        							"type": "keyword"
        						},
        						"username": {
        							"type": "keyword"
        						}
        					}
        				},
        				"externalReferenceAttachmentTypeId": {
        					"type": "keyword"
        				},
        				"externalReferenceId": {
        					"type": "keyword"
        				},
        				"externalReferenceMetadata": {
        					"type": "object",
        					"dynamic": "false",
        					"enabled": false
        				},
        				"externalReferenceStorage": {
        					"dynamic": "false",
        					"properties": {
        						"type": {
        							"type": "keyword"
        						}
        					}
        				},
        				"index": {
        					"type": "keyword"
        				},
        				"owner": {
        					"type": "keyword"
        				},
        				"persistableStateAttachmentState": {
        					"type": "object",
        					"dynamic": "false",
        					"enabled": false
        				},
        				"persistableStateAttachmentTypeId": {
        					"type": "keyword"
        				},
        				"pushed_at": {
        					"type": "date"
        				},
        				"pushed_by": {
        					"properties": {
        						"email": {
        							"type": "keyword"
        						},
        						"full_name": {
        							"type": "keyword"
        						},
        						"profile_uid": {
        							"type": "keyword"
        						},
        						"username": {
        							"type": "keyword"
        						}
        					}
        				},
        				"rule": {
        					"properties": {
        						"id": {
        							"type": "keyword"
        						},
        						"name": {
        							"type": "keyword"
        						}
        					}
        				},
        				"type": {
        					"type": "keyword"
        				},
        				"updated_at": {
        					"type": "date"
        				},
        				"updated_by": {
        					"properties": {
        						"email": {
        							"type": "keyword"
        						},
        						"full_name": {
        							"type": "keyword"
        						},
        						"profile_uid": {
        							"type": "keyword"
        						},
        						"username": {
        							"type": "keyword"
        						}
        					}
        				}
        			}
        		},
        		"cases-configure": {
        			"properties": {
        				"closure_type": {
        					"type": "keyword"
        				},
        				"connector": {
        					"properties": {
        						"fields": {
        							"properties": {
        								"key": {
        									"type": "text"
        								},
        								"value": {
        									"type": "text"
        								}
        							}
        						},
        						"name": {
        							"type": "text"
        						},
        						"type": {
        							"type": "keyword"
        						}
        					}
        				},
        				"created_at": {
        					"type": "date"
        				},
        				"created_by": {
        					"properties": {
        						"email": {
        							"type": "keyword"
        						},
        						"full_name": {
        							"type": "keyword"
        						},
        						"profile_uid": {
        							"type": "keyword"
        						},
        						"username": {
        							"type": "keyword"
        						}
        					}
        				},
        				"owner": {
        					"type": "keyword"
        				},
        				"updated_at": {
        					"type": "date"
        				},
        				"updated_by": {
        					"properties": {
        						"email": {
        							"type": "keyword"
        						},
        						"full_name": {
        							"type": "keyword"
        						},
        						"profile_uid": {
        							"type": "keyword"
        						},
        						"username": {
        							"type": "keyword"
        						}
        					}
        				}
        			}
        		},
        		"cases-connector-mappings": {
        			"properties": {
        				"mappings": {
        					"properties": {
        						"action_type": {
        							"type": "keyword"
        						},
        						"source": {
        							"type": "keyword"
        						},
        						"target": {
        							"type": "keyword"
        						}
        					}
        				},
        				"owner": {
        					"type": "keyword"
        				}
        			}
        		},
        		"cases-telemetry": {
        			"type": "object",
        			"dynamic": "false"
        		},
        		"cases-user-actions": {
        			"properties": {
        				"action": {
        					"type": "keyword"
        				},
        				"created_at": {
        					"type": "date"
        				},
        				"created_by": {
        					"properties": {
        						"email": {
        							"type": "keyword"
        						},
        						"full_name": {
        							"type": "keyword"
        						},
        						"profile_uid": {
        							"type": "keyword"
        						},
        						"username": {
        							"type": "keyword"
        						}
        					}
        				},
        				"owner": {
        					"type": "keyword"
        				},
        				"payload": {
        					"dynamic": "false",
        					"properties": {
        						"assignees": {
        							"properties": {
        								"uid": {
        									"type": "keyword"
        								}
        							}
        						},
        						"comment": {
        							"properties": {
        								"externalReferenceAttachmentTypeId": {
        									"type": "keyword"
        								},
        								"persistableStateAttachmentTypeId": {
        									"type": "keyword"
        								},
        								"type": {
        									"type": "keyword"
        								}
        							}
        						},
        						"connector": {
        							"properties": {
        								"type": {
        									"type": "keyword"
        								}
        							}
        						}
        					}
        				},
        				"type": {
        					"type": "keyword"
        				}
        			}
        		},
        		"config": {
        			"dynamic": "false",
        			"properties": {
        				"buildNum": {
        					"type": "keyword"
        				}
        			}
        		},
        		"config-global": {
        			"dynamic": "false",
        			"properties": {
        				"buildNum": {
        					"type": "keyword"
        				}
        			}
        		},
        		"connector_token": {
        			"properties": {
        				"connectorId": {
        					"type": "keyword"
        				},
        				"createdAt": {
        					"type": "date"
        				},
        				"expiresAt": {
        					"type": "date"
        				},
        				"token": {
        					"type": "binary"
        				},
        				"tokenType": {
        					"type": "keyword"
        				},
        				"updatedAt": {
        					"type": "date"
        				}
        			}
        		},
        		"core-usage-stats": {
        			"type": "object",
        			"dynamic": "false"
        		},
        		"coreMigrationVersion": {
        			"type": "keyword"
        		},
        		"created_at": {
        			"type": "date"
        		},
        		"csp-rule-template": {
        			"dynamic": "false",
        			"properties": {
        				"metadata": {
        					"properties": {
        						"benchmark": {
        							"properties": {
        								"id": {
        									"type": "keyword"
        								}
        							}
        						},
        						"name": {
        							"type": "keyword",
        							"fields": {
        								"text": {
        									"type": "text"
        								}
        							}
        						}
        					}
        				}
        			}
        		},
        		"dashboard": {
        			"properties": {
        				"controlGroupInput": {
        					"properties": {
        						"chainingSystem": {
        							"type": "keyword",
        							"index": false,
        							"doc_values": false
        						},
        						"controlStyle": {
        							"type": "keyword",
        							"index": false,
        							"doc_values": false
        						},
        						"ignoreParentSettingsJSON": {
        							"type": "text",
        							"index": false
        						},
        						"panelsJSON": {
        							"type": "text",
        							"index": false
        						}
        					}
        				},
        				"description": {
        					"type": "text"
        				},
        				"hits": {
        					"type": "integer",
        					"index": false,
        					"doc_values": false
        				},
        				"kibanaSavedObjectMeta": {
        					"properties": {
        						"searchSourceJSON": {
        							"type": "text",
        							"index": false
        						}
        					}
        				},
        				"optionsJSON": {
        					"type": "text",
        					"index": false
        				},
        				"panelsJSON": {
        					"type": "text",
        					"index": false
        				},
        				"refreshInterval": {
        					"properties": {
        						"display": {
        							"type": "keyword",
        							"index": false,
        							"doc_values": false
        						},
        						"pause": {
        							"type": "boolean",
        							"doc_values": false,
        							"index": false
        						},
        						"section": {
        							"type": "integer",
        							"index": false,
        							"doc_values": false
        						},
        						"value": {
        							"type": "integer",
        							"index": false,
        							"doc_values": false
        						}
        					}
        				},
        				"timeFrom": {
        					"type": "keyword",
        					"index": false,
        					"doc_values": false
        				},
        				"timeRestore": {
        					"type": "boolean",
        					"doc_values": false,
        					"index": false
        				},
        				"timeTo": {
        					"type": "keyword",
        					"index": false,
        					"doc_values": false
        				},
        				"title": {
        					"type": "text"
        				},
        				"version": {
        					"type": "integer"
        				}
        			}
        		},
        		"endpoint:user-artifact": {
        			"properties": {
        				"body": {
        					"type": "binary"
        				},
        				"compressionAlgorithm": {
        					"type": "keyword",
        					"index": false
        				},
        				"created": {
        					"type": "date",
        					"index": false
        				},
        				"decodedSha256": {
        					"type": "keyword",
        					"index": false
        				},
        				"decodedSize": {
        					"type": "long",
        					"index": false
        				},
        				"encodedSha256": {
        					"type": "keyword"
        				},
        				"encodedSize": {
        					"type": "long",
        					"index": false
        				},
        				"encryptionAlgorithm": {
        					"type": "keyword",
        					"index": false
        				},
        				"identifier": {
        					"type": "keyword"
        				}
        			}
        		},
        		"endpoint:user-artifact-manifest": {
        			"properties": {
        				"artifacts": {
        					"type": "nested",
        					"properties": {
        						"artifactId": {
        							"type": "keyword",
        							"index": false
        						},
        						"policyId": {
        							"type": "keyword",
        							"index": false
        						}
        					}
        				},
        				"created": {
        					"type": "date",
        					"index": false
        				},
        				"schemaVersion": {
        					"type": "keyword"
        				},
        				"semanticVersion": {
        					"type": "keyword",
        					"index": false
        				}
        			}
        		},
        		"enterprise_search_telemetry": {
        			"type": "object",
        			"dynamic": "false"
        		},
        		"epm-packages": {
        			"properties": {
        				"es_index_patterns": {
        					"type": "object",
        					"enabled": false
        				},
        				"experimental_data_stream_features": {
        					"type": "nested",
        					"properties": {
        						"data_stream": {
        							"type": "keyword"
        						},
        						"features": {
        							"type": "nested",
        							"dynamic": "false",
        							"properties": {
        								"synthetic_source": {
        									"type": "boolean"
        								},
        								"tsdb": {
        									"type": "boolean"
        								}
        							}
        						}
        					}
        				},
        				"install_source": {
        					"type": "keyword"
        				},
        				"install_started_at": {
        					"type": "date"
        				},
        				"install_status": {
        					"type": "keyword"
        				},
        				"install_version": {
        					"type": "keyword"
        				},
        				"installed_es": {
        					"type": "nested",
        					"properties": {
        						"id": {
        							"type": "keyword"
        						},
        						"type": {
        							"type": "keyword"
        						},
        						"version": {
        							"type": "keyword"
        						}
        					}
        				},
        				"installed_kibana": {
        					"type": "object",
        					"enabled": false
        				},
        				"installed_kibana_space_id": {
        					"type": "keyword"
        				},
        				"internal": {
        					"type": "boolean"
        				},
        				"keep_policies_up_to_date": {
        					"type": "boolean",
        					"index": false
        				},
        				"name": {
        					"type": "keyword"
        				},
        				"package_assets": {
        					"type": "object",
        					"enabled": false
        				},
        				"verification_key_id": {
        					"type": "keyword"
        				},
        				"verification_status": {
        					"type": "keyword"
        				},
        				"version": {
        					"type": "keyword"
        				}
        			}
        		},
        		"epm-packages-assets": {
        			"properties": {
        				"asset_path": {
        					"type": "keyword"
        				},
        				"data_base64": {
        					"type": "binary"
        				},
        				"data_utf8": {
        					"type": "text",
        					"index": false
        				},
        				"install_source": {
        					"type": "keyword"
        				},
        				"media_type": {
        					"type": "keyword"
        				},
        				"package_name": {
        					"type": "keyword"
        				},
        				"package_version": {
        					"type": "keyword"
        				}
        			}
        		},
        		"event_loop_delays_daily": {
        			"dynamic": "false",
        			"properties": {
        				"lastUpdatedAt": {
        					"type": "date"
        				}
        			}
        		},
        		"exception-list": {
        			"properties": {
        				"_tags": {
        					"type": "keyword"
        				},
        				"comments": {
        					"properties": {
        						"comment": {
        							"type": "keyword"
        						},
        						"created_at": {
        							"type": "keyword"
        						},
        						"created_by": {
        							"type": "keyword"
        						},
        						"id": {
        							"type": "keyword"
        						},
        						"updated_at": {
        							"type": "keyword"
        						},
        						"updated_by": {
        							"type": "keyword"
        						}
        					}
        				},
        				"created_at": {
        					"type": "keyword"
        				},
        				"created_by": {
        					"type": "keyword"
        				},
        				"description": {
        					"type": "keyword"
        				},
        				"entries": {
        					"properties": {
        						"entries": {
        							"properties": {
        								"field": {
        									"type": "keyword"
        								},
        								"operator": {
        									"type": "keyword"
        								},
        								"type": {
        									"type": "keyword"
        								},
        								"value": {
        									"type": "keyword",
        									"fields": {
        										"text": {
        											"type": "text"
        										}
        									}
        								}
        							}
        						},
        						"field": {
        							"type": "keyword"
        						},
        						"list": {
        							"properties": {
        								"id": {
        									"type": "keyword"
        								},
        								"type": {
        									"type": "keyword"
        								}
        							}
        						},
        						"operator": {
        							"type": "keyword"
        						},
        						"type": {
        							"type": "keyword"
        						},
        						"value": {
        							"type": "keyword",
        							"fields": {
        								"text": {
        									"type": "text"
        								}
        							}
        						}
        					}
        				},
        				"expire_time": {
        					"type": "date"
        				},
        				"immutable": {
        					"type": "boolean"
        				},
        				"item_id": {
        					"type": "keyword"
        				},
        				"list_id": {
        					"type": "keyword"
        				},
        				"list_type": {
        					"type": "keyword"
        				},
        				"meta": {
        					"type": "keyword"
        				},
        				"name": {
        					"type": "keyword",
        					"fields": {
        						"text": {
        							"type": "text"
        						}
        					}
        				},
        				"os_types": {
        					"type": "keyword"
        				},
        				"tags": {
        					"type": "keyword",
        					"fields": {
        						"text": {
        							"type": "text"
        						}
        					}
        				},
        				"tie_breaker_id": {
        					"type": "keyword"
        				},
        				"type": {
        					"type": "keyword"
        				},
        				"updated_by": {
        					"type": "keyword"
        				},
        				"version": {
        					"type": "keyword"
        				}
        			}
        		},
        		"exception-list-agnostic": {
        			"properties": {
        				"_tags": {
        					"type": "keyword"
        				},
        				"comments": {
        					"properties": {
        						"comment": {
        							"type": "keyword"
        						},
        						"created_at": {
        							"type": "keyword"
        						},
        						"created_by": {
        							"type": "keyword"
        						},
        						"id": {
        							"type": "keyword"
        						},
        						"updated_at": {
        							"type": "keyword"
        						},
        						"updated_by": {
        							"type": "keyword"
        						}
        					}
        				},
        				"created_at": {
        					"type": "keyword"
        				},
        				"created_by": {
        					"type": "keyword"
        				},
        				"description": {
        					"type": "keyword"
        				},
        				"entries": {
        					"properties": {
        						"entries": {
        							"properties": {
        								"field": {
        									"type": "keyword"
        								},
        								"operator": {
        									"type": "keyword"
        								},
        								"type": {
        									"type": "keyword"
        								},
        								"value": {
        									"type": "keyword",
        									"fields": {
        										"text": {
        											"type": "text"
        										}
        									}
        								}
        							}
        						},
        						"field": {
        							"type": "keyword"
        						},
        						"list": {
        							"properties": {
        								"id": {
        									"type": "keyword"
        								},
        								"type": {
        									"type": "keyword"
        								}
        							}
        						},
        						"operator": {
        							"type": "keyword"
        						},
        						"type": {
        							"type": "keyword"
        						},
        						"value": {
        							"type": "keyword",
        							"fields": {
        								"text": {
        									"type": "text"
        								}
        							}
        						}
        					}
        				},
        				"expire_time": {
        					"type": "date"
        				},
        				"immutable": {
        					"type": "boolean"
        				},
        				"item_id": {
        					"type": "keyword"
        				},
        				"list_id": {
        					"type": "keyword"
        				},
        				"list_type": {
        					"type": "keyword"
        				},
        				"meta": {
        					"type": "keyword"
        				},
        				"name": {
        					"type": "keyword",
        					"fields": {
        						"text": {
        							"type": "text"
        						}
        					}
        				},
        				"os_types": {
        					"type": "keyword"
        				},
        				"tags": {
        					"type": "keyword",
        					"fields": {
        						"text": {
        							"type": "text"
        						}
        					}
        				},
        				"tie_breaker_id": {
        					"type": "keyword"
        				},
        				"type": {
        					"type": "keyword"
        				},
        				"updated_by": {
        					"type": "keyword"
        				},
        				"version": {
        					"type": "keyword"
        				}
        			}
        		},
        		"file": {
        			"dynamic": "false",
        			"properties": {
        				"FileKind": {
        					"type": "keyword"
        				},
        				"Meta": {
        					"type": "flattened"
        				},
        				"Status": {
        					"type": "keyword"
        				},
        				"Updated": {
        					"type": "date"
        				},
        				"created": {
        					"type": "date"
        				},
        				"extension": {
        					"type": "keyword"
        				},
        				"mime_type": {
        					"type": "keyword"
        				},
        				"name": {
        					"type": "text"
        				},
        				"size": {
        					"type": "long"
        				},
        				"user": {
        					"type": "flattened"
        				}
        			}
        		},
        		"file-upload-usage-collection-telemetry": {
        			"properties": {
        				"file_upload": {
        					"properties": {
        						"index_creation_count": {
        							"type": "long"
        						}
        					}
        				}
        			}
        		},
        		"fileShare": {
        			"dynamic": "false",
        			"properties": {
        				"created": {
        					"type": "date"
        				},
        				"name": {
        					"type": "keyword"
        				},
        				"token": {
        					"type": "keyword"
        				},
        				"valid_until": {
        					"type": "long"
        				}
        			}
        		},
        		"fleet-fleet-server-host": {
        			"properties": {
        				"host_urls": {
        					"type": "keyword",
        					"index": false
        				},
        				"is_default": {
        					"type": "boolean"
        				},
        				"is_preconfigured": {
        					"type": "boolean"
        				},
        				"name": {
        					"type": "keyword"
        				},
        				"proxy_id": {
        					"type": "keyword"
        				}
        			}
        		},
        		"fleet-preconfiguration-deletion-record": {
        			"properties": {
        				"id": {
        					"type": "keyword"
        				}
        			}
        		},
        		"fleet-proxy": {
        			"properties": {
        				"certificate": {
        					"type": "keyword",
        					"index": false
        				},
        				"certificate_authorities": {
        					"type": "keyword",
        					"index": false
        				},
        				"certificate_key": {
        					"type": "keyword",
        					"index": false
        				},
        				"is_preconfigured": {
        					"type": "boolean"
        				},
        				"name": {
        					"type": "keyword"
        				},
        				"proxy_headers": {
        					"type": "text",
        					"index": false
        				},
        				"url": {
        					"type": "keyword",
        					"index": false
        				}
        			}
        		},
        		"graph-workspace": {
        			"properties": {
        				"description": {
        					"type": "text"
        				},
        				"kibanaSavedObjectMeta": {
        					"properties": {
        						"searchSourceJSON": {
        							"type": "text"
        						}
        					}
        				},
        				"legacyIndexPatternRef": {
        					"type": "text",
        					"index": false
        				},
        				"numLinks": {
        					"type": "integer"
        				},
        				"numVertices": {
        					"type": "integer"
        				},
        				"title": {
        					"type": "text"
        				},
        				"version": {
        					"type": "integer"
        				},
        				"wsState": {
        					"type": "text"
        				}
        			}
        		},
        		"guided-onboarding-guide-state": {
        			"dynamic": "false",
        			"properties": {
        				"guideId": {
        					"type": "keyword"
        				},
        				"isActive": {
        					"type": "boolean"
        				}
        			}
        		},
        		"guided-onboarding-plugin-state": {
        			"type": "object",
        			"dynamic": "false"
        		},
        		"index-pattern": {
        			"dynamic": "false",
        			"properties": {
        				"name": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword"
        						}
        					}
        				},
        				"title": {
        					"type": "text"
        				},
        				"type": {
        					"type": "keyword"
        				}
        			}
        		},
        		"infrastructure-monitoring-log-view": {
        			"dynamic": "false",
        			"properties": {
        				"name": {
        					"type": "text"
        				}
        			}
        		},
        		"infrastructure-ui-source": {
        			"type": "object",
        			"dynamic": "false"
        		},
        		"ingest-agent-policies": {
        			"properties": {
        				"agent_features": {
        					"properties": {
        						"enabled": {
        							"type": "boolean"
        						},
        						"name": {
        							"type": "keyword"
        						}
        					}
        				},
        				"data_output_id": {
        					"type": "keyword"
        				},
        				"description": {
        					"type": "text"
        				},
        				"download_source_id": {
        					"type": "keyword"
        				},
        				"fleet_server_host_id": {
        					"type": "keyword"
        				},
        				"inactivity_timeout": {
        					"type": "integer"
        				},
        				"is_default": {
        					"type": "boolean"
        				},
        				"is_default_fleet_server": {
        					"type": "boolean"
        				},
        				"is_managed": {
        					"type": "boolean"
        				},
        				"is_preconfigured": {
        					"type": "keyword"
        				},
        				"monitoring_enabled": {
        					"type": "keyword",
        					"index": false
        				},
        				"monitoring_output_id": {
        					"type": "keyword"
        				},
        				"name": {
        					"type": "keyword"
        				},
        				"namespace": {
        					"type": "keyword"
        				},
        				"revision": {
        					"type": "integer"
        				},
        				"status": {
        					"type": "keyword"
        				},
        				"unenroll_timeout": {
        					"type": "integer"
        				},
        				"updated_at": {
        					"type": "date"
        				},
        				"updated_by": {
        					"type": "keyword"
        				}
        			}
        		},
        		"ingest-download-sources": {
        			"properties": {
        				"host": {
        					"type": "keyword"
        				},
        				"is_default": {
        					"type": "boolean"
        				},
        				"name": {
        					"type": "keyword"
        				},
        				"source_id": {
        					"type": "keyword",
        					"index": false
        				}
        			}
        		},
        		"ingest-outputs": {
        			"properties": {
        				"ca_sha256": {
        					"type": "keyword",
        					"index": false
        				},
        				"ca_trusted_fingerprint": {
        					"type": "keyword",
        					"index": false
        				},
        				"config": {
        					"type": "flattened"
        				},
        				"config_yaml": {
        					"type": "text"
        				},
        				"hosts": {
        					"type": "keyword"
        				},
        				"is_default": {
        					"type": "boolean"
        				},
        				"is_default_monitoring": {
        					"type": "boolean"
        				},
        				"is_preconfigured": {
        					"type": "boolean",
        					"index": false
        				},
        				"name": {
        					"type": "keyword"
        				},
        				"output_id": {
        					"type": "keyword",
        					"index": false
        				},
        				"proxy_id": {
        					"type": "keyword"
        				},
        				"shipper": {
        					"type": "object",
        					"dynamic": "false"
        				},
        				"ssl": {
        					"type": "binary"
        				},
        				"type": {
        					"type": "keyword"
        				}
        			}
        		},
        		"ingest-package-policies": {
        			"properties": {
        				"created_at": {
        					"type": "date"
        				},
        				"created_by": {
        					"type": "keyword"
        				},
        				"description": {
        					"type": "text"
        				},
        				"elasticsearch": {
        					"enabled": false,
        					"properties": {
        						"privileges": {
        							"properties": {
        								"cluster": {
        									"type": "keyword"
        								}
        							}
        						}
        					}
        				},
        				"enabled": {
        					"type": "boolean"
        				},
        				"inputs": {
        					"type": "nested",
        					"enabled": false,
        					"properties": {
        						"compiled_input": {
        							"type": "flattened"
        						},
        						"config": {
        							"type": "flattened"
        						},
        						"enabled": {
        							"type": "boolean"
        						},
        						"policy_template": {
        							"type": "keyword"
        						},
        						"streams": {
        							"type": "nested",
        							"properties": {
        								"compiled_stream": {
        									"type": "flattened"
        								},
        								"config": {
        									"type": "flattened"
        								},
        								"data_stream": {
        									"properties": {
        										"dataset": {
        											"type": "keyword"
        										},
        										"elasticsearch": {
        											"properties": {
        												"privileges": {
        													"type": "flattened"
        												}
        											}
        										},
        										"type": {
        											"type": "keyword"
        										}
        									}
        								},
        								"enabled": {
        									"type": "boolean"
        								},
        								"id": {
        									"type": "keyword"
        								},
        								"vars": {
        									"type": "flattened"
        								}
        							}
        						},
        						"type": {
        							"type": "keyword"
        						},
        						"vars": {
        							"type": "flattened"
        						}
        					}
        				},
        				"is_managed": {
        					"type": "boolean"
        				},
        				"name": {
        					"type": "keyword"
        				},
        				"namespace": {
        					"type": "keyword"
        				},
        				"package": {
        					"properties": {
        						"name": {
        							"type": "keyword"
        						},
        						"title": {
        							"type": "keyword"
        						},
        						"version": {
        							"type": "keyword"
        						}
        					}
        				},
        				"policy_id": {
        					"type": "keyword"
        				},
        				"revision": {
        					"type": "integer"
        				},
        				"updated_at": {
        					"type": "date"
        				},
        				"updated_by": {
        					"type": "keyword"
        				},
        				"vars": {
        					"type": "flattened"
        				}
        			}
        		},
        		"ingest_manager_settings": {
        			"properties": {
        				"fleet_server_hosts": {
        					"type": "keyword"
        				},
        				"has_seen_add_data_notice": {
        					"type": "boolean",
        					"index": false
        				},
        				"prerelease_integrations_enabled": {
        					"type": "boolean"
        				}
        			}
        		},
        		"inventory-view": {
        			"type": "object",
        			"dynamic": "false"
        		},
        		"kql-telemetry": {
        			"properties": {
        				"optInCount": {
        					"type": "long"
        				},
        				"optOutCount": {
        					"type": "long"
        				}
        			}
        		},
        		"legacy-url-alias": {
        			"dynamic": "false",
        			"properties": {
        				"disabled": {
        					"type": "boolean"
        				},
        				"resolveCounter": {
        					"type": "long"
        				},
        				"sourceId": {
        					"type": "keyword"
        				},
        				"targetId": {
        					"type": "keyword"
        				},
        				"targetNamespace": {
        					"type": "keyword"
        				},
        				"targetType": {
        					"type": "keyword"
        				}
        			}
        		},
        		"lens": {
        			"properties": {
        				"description": {
        					"type": "text"
        				},
        				"expression": {
        					"type": "keyword",
        					"index": false,
        					"doc_values": false
        				},
        				"state": {
        					"type": "flattened"
        				},
        				"title": {
        					"type": "text"
        				},
        				"visualizationType": {
        					"type": "keyword"
        				}
        			}
        		},
        		"lens-ui-telemetry": {
        			"properties": {
        				"count": {
        					"type": "integer"
        				},
        				"date": {
        					"type": "date"
        				},
        				"name": {
        					"type": "keyword"
        				},
        				"type": {
        					"type": "keyword"
        				}
        			}
        		},
        		"map": {
        			"properties": {
        				"bounds": {
        					"type": "object",
        					"dynamic": "false"
        				},
        				"description": {
        					"type": "text"
        				},
        				"layerListJSON": {
        					"type": "text"
        				},
        				"mapStateJSON": {
        					"type": "text"
        				},
        				"title": {
        					"type": "text"
        				},
        				"uiStateJSON": {
        					"type": "text"
        				},
        				"version": {
        					"type": "integer"
        				}
        			}
        		},
        		"maps-telemetry": {
        			"type": "object",
        			"enabled": false
        		},
        		"metrics-explorer-view": {
        			"type": "object",
        			"dynamic": "false"
        		},
        		"migrationVersion": {
        			"dynamic": "true",
        			"properties": {
        				"canvas-workpad-template": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256
        						}
        					}
        				},
        				"config": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256
        						}
        					}
        				},
        				"core-usage-stats": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256
        						}
        					}
        				},
        				"exception-list-agnostic": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256
        						}
        					}
        				},
        				"search-telemetry": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256
        						}
        					}
        				},
        				"space": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256
        						}
        					}
        				},
        				"spaces-usage-stats": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256
        						}
        					}
        				}
        			}
        		},
        		"ml-job": {
        			"properties": {
        				"datafeed_id": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword"
        						}
        					}
        				},
        				"job_id": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword"
        						}
        					}
        				},
        				"type": {
        					"type": "keyword"
        				}
        			}
        		},
        		"ml-module": {
        			"dynamic": "false",
        			"properties": {
        				"datafeeds": {
        					"type": "object"
        				},
        				"defaultIndexPattern": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword"
        						}
        					}
        				},
        				"description": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword"
        						}
        					}
        				},
        				"id": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword"
        						}
        					}
        				},
        				"jobs": {
        					"type": "object"
        				},
        				"logo": {
        					"type": "object"
        				},
        				"query": {
        					"type": "object"
        				},
        				"title": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword"
        						}
        					}
        				},
        				"type": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword"
        						}
        					}
        				}
        			}
        		},
        		"ml-trained-model": {
        			"properties": {
        				"job": {
        					"properties": {
        						"create_time": {
        							"type": "date"
        						},
        						"job_id": {
        							"type": "text",
        							"fields": {
        								"keyword": {
        									"type": "keyword"
        								}
        							}
        						}
        					}
        				},
        				"model_id": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword"
        						}
        					}
        				}
        			}
        		},
        		"monitoring-telemetry": {
        			"properties": {
        				"reportedClusterUuids": {
        					"type": "keyword"
        				}
        			}
        		},
        		"namespace": {
        			"type": "keyword"
        		},
        		"namespaces": {
        			"type": "keyword"
        		},
        		"originId": {
        			"type": "keyword"
        		},
        		"osquery-manager-usage-metric": {
        			"properties": {
        				"count": {
        					"type": "long"
        				},
        				"errors": {
        					"type": "long"
        				}
        			}
        		},
        		"osquery-pack": {
        			"properties": {
        				"created_at": {
        					"type": "date"
        				},
        				"created_by": {
        					"type": "keyword"
        				},
        				"description": {
        					"type": "text"
        				},
        				"enabled": {
        					"type": "boolean"
        				},
        				"name": {
        					"type": "text"
        				},
        				"queries": {
        					"dynamic": "false",
        					"properties": {
        						"ecs_mapping": {
        							"type": "object",
        							"enabled": false
        						},
        						"id": {
        							"type": "keyword"
        						},
        						"interval": {
        							"type": "text"
        						},
        						"platform": {
        							"type": "keyword"
        						},
        						"query": {
        							"type": "text"
        						},
        						"version": {
        							"type": "keyword"
        						}
        					}
        				},
        				"shards": {
        					"type": "object",
        					"enabled": false
        				},
        				"updated_at": {
        					"type": "date"
        				},
        				"updated_by": {
        					"type": "keyword"
        				},
        				"version": {
        					"type": "long"
        				}
        			}
        		},
        		"osquery-pack-asset": {
        			"dynamic": "false",
        			"properties": {
        				"description": {
        					"type": "text"
        				},
        				"name": {
        					"type": "text"
        				},
        				"queries": {
        					"dynamic": "false",
        					"properties": {
        						"ecs_mapping": {
        							"type": "object",
        							"enabled": false
        						},
        						"id": {
        							"type": "keyword"
        						},
        						"interval": {
        							"type": "text"
        						},
        						"platform": {
        							"type": "keyword"
        						},
        						"query": {
        							"type": "text"
        						},
        						"version": {
        							"type": "keyword"
        						}
        					}
        				},
        				"shards": {
        					"type": "object",
        					"enabled": false
        				},
        				"version": {
        					"type": "long"
        				}
        			}
        		},
        		"osquery-saved-query": {
        			"dynamic": "false",
        			"properties": {
        				"created_at": {
        					"type": "date"
        				},
        				"created_by": {
        					"type": "text"
        				},
        				"description": {
        					"type": "text"
        				},
        				"ecs_mapping": {
        					"type": "object",
        					"enabled": false
        				},
        				"id": {
        					"type": "keyword"
        				},
        				"interval": {
        					"type": "keyword"
        				},
        				"platform": {
        					"type": "keyword"
        				},
        				"query": {
        					"type": "text"
        				},
        				"updated_at": {
        					"type": "date"
        				},
        				"updated_by": {
        					"type": "text"
        				},
        				"version": {
        					"type": "keyword"
        				}
        			}
        		},
        		"query": {
        			"properties": {
        				"description": {
        					"type": "text"
        				},
        				"filters": {
        					"type": "object",
        					"enabled": false
        				},
        				"query": {
        					"properties": {
        						"language": {
        							"type": "keyword"
        						},
        						"query": {
        							"type": "keyword",
        							"index": false
        						}
        					}
        				},
        				"timefilter": {
        					"type": "object",
        					"enabled": false
        				},
        				"title": {
        					"type": "text"
        				}
        			}
        		},
        		"references": {
        			"type": "nested",
        			"properties": {
        				"id": {
        					"type": "keyword"
        				},
        				"name": {
        					"type": "keyword"
        				},
        				"type": {
        					"type": "keyword"
        				}
        			}
        		},
        		"rules-settings": {
        			"properties": {
        				"flapping": {
        					"properties": {
        						"createdAt": {
        							"type": "date",
        							"index": false
        						},
        						"createdBy": {
        							"type": "keyword",
        							"index": false
        						},
        						"enabled": {
        							"type": "boolean",
        							"index": false
        						},
        						"lookBackWindow": {
        							"type": "long",
        							"index": false
        						},
        						"statusChangeThreshold": {
        							"type": "long",
        							"index": false
        						},
        						"updatedAt": {
        							"type": "date",
        							"index": false
        						},
        						"updatedBy": {
        							"type": "keyword",
        							"index": false
        						}
        					}
        				}
        			}
        		},
        		"sample-data-telemetry": {
        			"properties": {
        				"installCount": {
        					"type": "long"
        				},
        				"unInstallCount": {
        					"type": "long"
        				}
        			}
        		},
        		"search": {
        			"properties": {
        				"breakdownField": {
        					"type": "text"
        				},
        				"columns": {
        					"type": "keyword",
        					"index": false,
        					"doc_values": false
        				},
        				"description": {
        					"type": "text"
        				},
        				"grid": {
        					"type": "object",
        					"enabled": false
        				},
        				"hideAggregatedPreview": {
        					"type": "boolean",
        					"doc_values": false,
        					"index": false
        				},
        				"hideChart": {
        					"type": "boolean",
        					"doc_values": false,
        					"index": false
        				},
        				"hits": {
        					"type": "integer",
        					"index": false,
        					"doc_values": false
        				},
        				"isTextBasedQuery": {
        					"type": "boolean",
        					"doc_values": false,
        					"index": false
        				},
        				"kibanaSavedObjectMeta": {
        					"properties": {
        						"searchSourceJSON": {
        							"type": "text",
        							"index": false
        						}
        					}
        				},
        				"refreshInterval": {
        					"dynamic": "false",
        					"properties": {
        						"pause": {
        							"type": "boolean",
        							"doc_values": false,
        							"index": false
        						},
        						"value": {
        							"type": "integer",
        							"index": false,
        							"doc_values": false
        						}
        					}
        				},
        				"rowHeight": {
        					"type": "text"
        				},
        				"rowsPerPage": {
        					"type": "integer",
        					"index": false,
        					"doc_values": false
        				},
        				"sort": {
        					"type": "keyword",
        					"index": false,
        					"doc_values": false
        				},
        				"timeRange": {
        					"dynamic": "false",
        					"properties": {
        						"from": {
        							"type": "keyword",
        							"index": false,
        							"doc_values": false
        						},
        						"to": {
        							"type": "keyword",
        							"index": false,
        							"doc_values": false
        						}
        					}
        				},
        				"timeRestore": {
        					"type": "boolean",
        					"doc_values": false,
        					"index": false
        				},
        				"title": {
        					"type": "text"
        				},
        				"usesAdHocDataView": {
        					"type": "boolean",
        					"doc_values": false,
        					"index": false
        				},
        				"version": {
        					"type": "integer"
        				},
        				"viewMode": {
        					"type": "keyword",
        					"index": false,
        					"doc_values": false
        				}
        			}
        		},
        		"search-session": {
        			"properties": {
        				"appId": {
        					"type": "keyword"
        				},
        				"created": {
        					"type": "date"
        				},
        				"expires": {
        					"type": "date"
        				},
        				"idMapping": {
        					"type": "object",
        					"enabled": false
        				},
        				"initialState": {
        					"type": "object",
        					"enabled": false
        				},
        				"isCanceled": {
        					"type": "boolean"
        				},
        				"locatorId": {
        					"type": "keyword"
        				},
        				"name": {
        					"type": "keyword"
        				},
        				"realmName": {
        					"type": "keyword"
        				},
        				"realmType": {
        					"type": "keyword"
        				},
        				"restoreState": {
        					"type": "object",
        					"enabled": false
        				},
        				"sessionId": {
        					"type": "keyword"
        				},
        				"username": {
        					"type": "keyword"
        				},
        				"version": {
        					"type": "keyword"
        				}
        			}
        		},
        		"search-telemetry": {
        			"type": "object",
        			"dynamic": "false"
        		},
        		"security-rule": {
        			"dynamic": "false",
        			"properties": {
        				"name": {
        					"type": "keyword"
        				},
        				"rule_id": {
        					"type": "keyword"
        				},
        				"version": {
        					"type": "long"
        				}
        			}
        		},
        		"security-solution-signals-migration": {
        			"properties": {
        				"created": {
        					"type": "date",
        					"index": false
        				},
        				"createdBy": {
        					"type": "text",
        					"index": false
        				},
        				"destinationIndex": {
        					"type": "keyword",
        					"index": false
        				},
        				"error": {
        					"type": "text",
        					"index": false
        				},
        				"sourceIndex": {
        					"type": "keyword"
        				},
        				"status": {
        					"type": "keyword",
        					"index": false
        				},
        				"taskId": {
        					"type": "keyword",
        					"index": false
        				},
        				"updated": {
        					"type": "date",
        					"index": false
        				},
        				"updatedBy": {
        					"type": "text",
        					"index": false
        				},
        				"version": {
        					"type": "long"
        				}
        			}
        		},
        		"siem-detection-engine-rule-actions": {
        			"properties": {
        				"actions": {
        					"properties": {
        						"actionRef": {
        							"type": "keyword"
        						},
        						"action_type_id": {
        							"type": "keyword"
        						},
        						"group": {
        							"type": "keyword"
        						},
        						"id": {
        							"type": "keyword"
        						},
        						"params": {
        							"type": "object",
        							"enabled": false
        						}
        					}
        				},
        				"alertThrottle": {
        					"type": "keyword"
        				},
        				"ruleAlertId": {
        					"type": "keyword"
        				},
        				"ruleThrottle": {
        					"type": "keyword"
        				}
        			}
        		},
        		"siem-ui-timeline": {
        			"properties": {
        				"columns": {
        					"properties": {
        						"aggregatable": {
        							"type": "boolean"
        						},
        						"category": {
        							"type": "keyword"
        						},
        						"columnHeaderType": {
        							"type": "keyword"
        						},
        						"description": {
        							"type": "text"
        						},
        						"example": {
        							"type": "text"
        						},
        						"id": {
        							"type": "keyword"
        						},
        						"indexes": {
        							"type": "keyword"
        						},
        						"name": {
        							"type": "text"
        						},
        						"placeholder": {
        							"type": "text"
        						},
        						"searchable": {
        							"type": "boolean"
        						},
        						"type": {
        							"type": "keyword"
        						}
        					}
        				},
        				"created": {
        					"type": "date"
        				},
        				"createdBy": {
        					"type": "text"
        				},
        				"dataProviders": {
        					"properties": {
        						"and": {
        							"properties": {
        								"enabled": {
        									"type": "boolean"
        								},
        								"excluded": {
        									"type": "boolean"
        								},
        								"id": {
        									"type": "keyword"
        								},
        								"kqlQuery": {
        									"type": "text"
        								},
        								"name": {
        									"type": "text"
        								},
        								"queryMatch": {
        									"properties": {
        										"displayField": {
        											"type": "text"
        										},
        										"displayValue": {
        											"type": "text"
        										},
        										"field": {
        											"type": "text"
        										},
        										"operator": {
        											"type": "text"
        										},
        										"value": {
        											"type": "text"
        										}
        									}
        								},
        								"type": {
        									"type": "text"
        								}
        							}
        						},
        						"enabled": {
        							"type": "boolean"
        						},
        						"excluded": {
        							"type": "boolean"
        						},
        						"id": {
        							"type": "keyword"
        						},
        						"kqlQuery": {
        							"type": "text"
        						},
        						"name": {
        							"type": "text"
        						},
        						"queryMatch": {
        							"properties": {
        								"displayField": {
        									"type": "text"
        								},
        								"displayValue": {
        									"type": "text"
        								},
        								"field": {
        									"type": "text"
        								},
        								"operator": {
        									"type": "text"
        								},
        								"value": {
        									"type": "text"
        								}
        							}
        						},
        						"type": {
        							"type": "text"
        						}
        					}
        				},
        				"dateRange": {
        					"properties": {
        						"end": {
        							"type": "date"
        						},
        						"start": {
        							"type": "date"
        						}
        					}
        				},
        				"description": {
        					"type": "text"
        				},
        				"eqlOptions": {
        					"properties": {
        						"eventCategoryField": {
        							"type": "text"
        						},
        						"query": {
        							"type": "text"
        						},
        						"size": {
        							"type": "text"
        						},
        						"tiebreakerField": {
        							"type": "text"
        						},
        						"timestampField": {
        							"type": "text"
        						}
        					}
        				},
        				"eventType": {
        					"type": "keyword"
        				},
        				"excludedRowRendererIds": {
        					"type": "text"
        				},
        				"favorite": {
        					"properties": {
        						"favoriteDate": {
        							"type": "date"
        						},
        						"fullName": {
        							"type": "text"
        						},
        						"keySearch": {
        							"type": "text"
        						},
        						"userName": {
        							"type": "text"
        						}
        					}
        				},
        				"filters": {
        					"properties": {
        						"exists": {
        							"type": "text"
        						},
        						"match_all": {
        							"type": "text"
        						},
        						"meta": {
        							"properties": {
        								"alias": {
        									"type": "text"
        								},
        								"controlledBy": {
        									"type": "text"
        								},
        								"disabled": {
        									"type": "boolean"
        								},
        								"field": {
        									"type": "text"
        								},
        								"formattedValue": {
        									"type": "text"
        								},
        								"index": {
        									"type": "keyword"
        								},
        								"key": {
        									"type": "keyword"
        								},
        								"negate": {
        									"type": "boolean"
        								},
        								"params": {
        									"type": "text"
        								},
        								"type": {
        									"type": "keyword"
        								},
        								"value": {
        									"type": "text"
        								}
        							}
        						},
        						"missing": {
        							"type": "text"
        						},
        						"query": {
        							"type": "text"
        						},
        						"range": {
        							"type": "text"
        						},
        						"script": {
        							"type": "text"
        						}
        					}
        				},
        				"indexNames": {
        					"type": "text"
        				},
        				"kqlMode": {
        					"type": "keyword"
        				},
        				"kqlQuery": {
        					"properties": {
        						"filterQuery": {
        							"properties": {
        								"kuery": {
        									"properties": {
        										"expression": {
        											"type": "text"
        										},
        										"kind": {
        											"type": "keyword"
        										}
        									}
        								},
        								"serializedQuery": {
        									"type": "text"
        								}
        							}
        						}
        					}
        				},
        				"sort": {
        					"dynamic": "false",
        					"properties": {
        						"columnId": {
        							"type": "keyword"
        						},
        						"columnType": {
        							"type": "keyword"
        						},
        						"sortDirection": {
        							"type": "keyword"
        						}
        					}
        				},
        				"status": {
        					"type": "keyword"
        				},
        				"templateTimelineId": {
        					"type": "text"
        				},
        				"templateTimelineVersion": {
        					"type": "integer"
        				},
        				"timelineType": {
        					"type": "keyword"
        				},
        				"title": {
        					"type": "text"
        				},
        				"updated": {
        					"type": "date"
        				},
        				"updatedBy": {
        					"type": "text"
        				}
        			}
        		},
        		"siem-ui-timeline-note": {
        			"properties": {
        				"created": {
        					"type": "date"
        				},
        				"createdBy": {
        					"type": "text"
        				},
        				"eventId": {
        					"type": "keyword"
        				},
        				"note": {
        					"type": "text"
        				},
        				"updated": {
        					"type": "date"
        				},
        				"updatedBy": {
        					"type": "text"
        				}
        			}
        		},
        		"siem-ui-timeline-pinned-event": {
        			"properties": {
        				"created": {
        					"type": "date"
        				},
        				"createdBy": {
        					"type": "text"
        				},
        				"eventId": {
        					"type": "keyword"
        				},
        				"updated": {
        					"type": "date"
        				},
        				"updatedBy": {
        					"type": "text"
        				}
        			}
        		},
        		"space": {
        			"properties": {
        				"_reserved": {
        					"type": "boolean"
        				},
        				"color": {
        					"type": "keyword"
        				},
        				"description": {
        					"type": "text"
        				},
        				"disabledFeatures": {
        					"type": "keyword"
        				},
        				"imageUrl": {
        					"type": "text",
        					"index": false
        				},
        				"initials": {
        					"type": "keyword"
        				},
        				"name": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 2048
        						}
        					}
        				}
        			}
        		},
        		"spaces-usage-stats": {
        			"type": "object",
        			"dynamic": "false"
        		},
        		"synthetics-monitor": {
        			"dynamic": "false",
        			"properties": {
        				"alert": {
        					"properties": {
        						"status": {
        							"properties": {
        								"enabled": {
        									"type": "boolean"
        								}
        							}
        						}
        					}
        				},
        				"custom_heartbeat_id": {
        					"type": "keyword"
        				},
        				"enabled": {
        					"type": "boolean"
        				},
        				"hash": {
        					"type": "keyword"
        				},
        				"hosts": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256
        						}
        					}
        				},
        				"id": {
        					"type": "keyword"
        				},
        				"journey_id": {
        					"type": "keyword"
        				},
        				"locations": {
        					"properties": {
        						"id": {
        							"type": "keyword",
        							"ignore_above": 256,
        							"fields": {
        								"text": {
        									"type": "text"
        								}
        							}
        						},
        						"label": {
        							"type": "text"
        						}
        					}
        				},
        				"name": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256,
        							"normalizer": "lowercase"
        						}
        					}
        				},
        				"origin": {
        					"type": "keyword"
        				},
        				"project_id": {
        					"type": "keyword",
        					"fields": {
        						"text": {
        							"type": "text"
        						}
        					}
        				},
        				"schedule": {
        					"properties": {
        						"number": {
        							"type": "integer"
        						}
        					}
        				},
        				"tags": {
        					"type": "keyword",
        					"fields": {
        						"text": {
        							"type": "text"
        						}
        					}
        				},
        				"type": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256
        						}
        					}
        				},
        				"urls": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256
        						}
        					}
        				}
        			}
        		},
        		"synthetics-param": {
        			"type": "object",
        			"dynamic": "false"
        		},
        		"synthetics-privates-locations": {
        			"type": "object",
        			"dynamic": "false"
        		},
        		"tag": {
        			"properties": {
        				"color": {
        					"type": "text"
        				},
        				"description": {
        					"type": "text"
        				},
        				"name": {
        					"type": "text"
        				}
        			}
        		},
        		"telemetry": {
        			"properties": {
        				"allowChangingOptInStatus": {
        					"type": "boolean"
        				},
        				"enabled": {
        					"type": "boolean"
        				},
        				"lastReported": {
        					"type": "date"
        				},
        				"lastVersionChecked": {
        					"type": "keyword"
        				},
        				"reportFailureCount": {
        					"type": "integer"
        				},
        				"reportFailureVersion": {
        					"type": "keyword"
        				},
        				"sendUsageFrom": {
        					"type": "keyword"
        				},
        				"userHasSeenNotice": {
        					"type": "boolean"
        				}
        			}
        		},
        		"type": {
        			"type": "keyword"
        		},
        		"ui-metric": {
        			"properties": {
        				"count": {
        					"type": "integer"
        				}
        			}
        		},
        		"updated_at": {
        			"type": "date"
        		},
        		"upgrade-assistant-ml-upgrade-operation": {
        			"properties": {
        				"jobId": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256
        						}
        					}
        				},
        				"nodeId": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256
        						}
        					}
        				},
        				"snapshotId": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256
        						}
        					}
        				},
        				"status": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256
        						}
        					}
        				}
        			}
        		},
        		"upgrade-assistant-reindex-operation": {
        			"properties": {
        				"errorMessage": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256
        						}
        					}
        				},
        				"indexName": {
        					"type": "keyword"
        				},
        				"lastCompletedStep": {
        					"type": "long"
        				},
        				"locked": {
        					"type": "date"
        				},
        				"newIndexName": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256
        						}
        					}
        				},
        				"reindexOptions": {
        					"properties": {
        						"openAndClose": {
        							"type": "boolean"
        						},
        						"queueSettings": {
        							"properties": {
        								"queuedAt": {
        									"type": "long"
        								},
        								"startedAt": {
        									"type": "long"
        								}
        							}
        						}
        					}
        				},
        				"reindexTaskId": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 256
        						}
        					}
        				},
        				"reindexTaskPercComplete": {
        					"type": "float"
        				},
        				"runningReindexCount": {
        					"type": "integer"
        				},
        				"status": {
        					"type": "integer"
        				}
        			}
        		},
        		"upgrade-assistant-telemetry": {
        			"properties": {
        				"features": {
        					"properties": {
        						"deprecation_logging": {
        							"properties": {
        								"enabled": {
        									"type": "boolean",
        									"null_value": true
        								}
        							}
        						}
        					}
        				}
        			}
        		},
        		"uptime-dynamic-settings": {
        			"type": "object",
        			"dynamic": "false"
        		},
        		"uptime-synthetics-api-key": {
        			"dynamic": "false",
        			"properties": {
        				"apiKey": {
        					"type": "binary"
        				}
        			}
        		},
        		"url": {
        			"properties": {
        				"accessCount": {
        					"type": "long"
        				},
        				"accessDate": {
        					"type": "date"
        				},
        				"createDate": {
        					"type": "date"
        				},
        				"locatorJSON": {
        					"type": "text",
        					"index": false
        				},
        				"slug": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword"
        						}
        					}
        				},
        				"url": {
        					"type": "text",
        					"fields": {
        						"keyword": {
        							"type": "keyword",
        							"ignore_above": 2048
        						}
        					}
        				}
        			}
        		},
        		"usage-counters": {
        			"dynamic": "false",
        			"properties": {
        				"domainId": {
        					"type": "keyword"
        				}
        			}
        		},
        		"visualization": {
        			"properties": {
        				"description": {
        					"type": "text"
        				},
        				"kibanaSavedObjectMeta": {
        					"properties": {
        						"searchSourceJSON": {
        							"type": "text",
        							"index": false
        						}
        					}
        				},
        				"savedSearchRefName": {
        					"type": "keyword",
        					"index": false,
        					"doc_values": false
        				},
        				"title": {
        					"type": "text"
        				},
        				"uiStateJSON": {
        					"type": "text",
        					"index": false
        				},
        				"version": {
        					"type": "integer"
        				},
        				"visState": {
        					"type": "text",
        					"index": false
        				}
        			}
        		},
        		"workplace_search_telemetry": {
        			"type": "object",
        			"dynamic": "false"
        		}
        	}
        }
        """;
}
