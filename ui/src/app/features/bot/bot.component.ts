import { Component, inject, ViewChild, ElementRef, AfterViewChecked } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { BotService, ChatMessage } from '../../core/services/bot.service';

@Component({
  selector: 'app-bot',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterLink],
  template: `
    <div class="page">
      <header><a routerLink="/" class="back">← Dashboard</a><h1>Supervisor Bot</h1></header>
      <div class="chat-container">
        <div class="messages" #scrollArea>
          @for (msg of messages(); track msg.id) {
            <div [class]="'msg msg-' + msg.role">
              <div class="msg-content">{{ msg.content }}</div>
              @if (msg.reply?.cards?.length) {
                <div class="cards">
                  @for (card of msg.reply!.cards; track card.title) {
                    <div class="card">
                      <h4>{{ card.title }}</h4>
                      @if (card.subtitle) { <p class="subtitle">{{ card.subtitle }}</p> }
                      <div class="card-data">
                        @for (entry of objectEntries(card.data); track entry[0]) {
                          <div class="data-row"><span class="key">{{ entry[0] }}:</span> <span>{{ entry[1] }}</span></div>
                        }
                      </div>
                    </div>
                  }
                </div>
              }
            </div>
          } @empty {
            <div class="empty-chat">
              <p>Ask me anything about your school data!</p>
              <p class="examples">Try: "Show today's sales summary" or "Top products" or "Help"</p>
            </div>
          }
          @if (loading()) { <div class="msg msg-bot"><div class="msg-content typing">Thinking...</div></div> }
        </div>
        <form class="input-bar" (ngSubmit)="send()">
          <input [(ngModel)]="query" name="query" placeholder="Ask the supervisor bot..." [disabled]="loading()" autocomplete="off" />
          <button type="submit" [disabled]="!query.trim() || loading()">Send</button>
        </form>
      </div>
    </div>
  `,
  styles: [`
    .page{padding:2rem;max-width:900px;margin:0 auto;display:flex;flex-direction:column;height:calc(100vh - 4rem)}
    header{margin-bottom:1rem}.back{color:#667eea;text-decoration:none;display:inline-block;margin-bottom:.5rem}h1{margin:0}
    .chat-container{flex:1;display:flex;flex-direction:column;background:#fff;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,.1);overflow:hidden}
    .messages{flex:1;overflow-y:auto;padding:1.5rem;display:flex;flex-direction:column;gap:1rem}
    .msg{max-width:80%;padding:.75rem 1rem;border-radius:12px;line-height:1.5}
    .msg-user{align-self:flex-end;background:#667eea;color:#fff}
    .msg-bot{align-self:flex-start;background:#f0f0f0;color:#333}
    .typing{font-style:italic;color:#999}
    .cards{display:flex;flex-wrap:wrap;gap:.75rem;margin-top:.75rem}
    .card{background:#fff;border:1px solid #ddd;border-radius:8px;padding:.75rem;min-width:200px;flex:1}
    .card h4{margin:0 0 .25rem}.subtitle{color:#666;font-size:.85rem;margin:0 0 .5rem}
    .data-row{font-size:.9rem;padding:.15rem 0}.key{font-weight:600;color:#555}
    .empty-chat{text-align:center;color:#999;margin:auto}.examples{font-size:.85rem;margin-top:.5rem}
    .input-bar{display:flex;border-top:1px solid #eee;padding:1rem;gap:.75rem}
    .input-bar input{flex:1;padding:.75rem;border:1px solid #ddd;border-radius:8px;font-size:1rem}
    .input-bar button{padding:.75rem 1.5rem;background:#667eea;color:#fff;border:none;border-radius:8px;cursor:pointer;font-size:1rem}
    .input-bar button:disabled{opacity:.5;cursor:not-allowed}
  `]
})
export class BotComponent implements AfterViewChecked {
  @ViewChild('scrollArea') scrollArea!: ElementRef;
  private botService = inject(BotService);
  query = '';
  messages = this.botService.messages;
  loading = this.botService.loading;

  ngAfterViewChecked() {
    if (this.scrollArea) {
      this.scrollArea.nativeElement.scrollTop = this.scrollArea.nativeElement.scrollHeight;
    }
  }

  send(): void {
    if (!this.query.trim()) return;
    const q = this.query;
    this.query = '';
    this.botService.sendQuery(q).subscribe();
  }

  objectEntries(obj: Record<string, string>): [string, string][] {
    return Object.entries(obj);
  }
}
