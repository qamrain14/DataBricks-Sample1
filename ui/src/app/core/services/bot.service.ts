import { Injectable, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, tap } from 'rxjs';
import { BotQueryRequest, BotReply } from '../models';

export interface ChatMessage {
  id: string;
  role: 'user' | 'bot';
  content: string;
  timestamp: Date;
  reply?: BotReply;
}

@Injectable({ providedIn: 'root' })
export class BotService {
  private readonly API_URL = 'http://localhost:5154/api/bot';
  
  private messagesSignal = signal<ChatMessage[]>([]);
  private loadingSignal = signal(false);

  readonly messages = this.messagesSignal.asReadonly();
  readonly loading = this.loadingSignal.asReadonly();

  constructor(private http: HttpClient) {}

  sendQuery(query: string): Observable<BotReply> {
    const userMessage: ChatMessage = {
      id: crypto.randomUUID(),
      role: 'user',
      content: query,
      timestamp: new Date()
    };
    
    this.messagesSignal.update(msgs => [...msgs, userMessage]);
    this.loadingSignal.set(true);

    const request: BotQueryRequest = { query };
    
    return this.http.post<BotReply>(`${this.API_URL}/query`, request).pipe(
      tap(reply => {
        const botMessage: ChatMessage = {
          id: crypto.randomUUID(),
          role: 'bot',
          content: reply.text,
          timestamp: new Date(),
          reply
        };
        this.messagesSignal.update(msgs => [...msgs, botMessage]);
        this.loadingSignal.set(false);
      })
    );
  }

  clearChat(): void {
    this.messagesSignal.set([]);
  }
}
