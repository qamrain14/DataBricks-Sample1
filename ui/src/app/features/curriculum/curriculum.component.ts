import { Component, signal, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink } from '@angular/router';
import { CurriculumService } from '../../core/services/curriculum.service';
import { ClassDto, SubjectDto, SectionDto, TimelineDto } from '../../core/models';

@Component({
  selector: 'app-curriculum',
  standalone: true,
  imports: [CommonModule, RouterLink],
  template: `
    <div class="page">
      <header><a routerLink="/" class="back">← Dashboard</a><h1>Curriculum Management</h1></header>
      <div class="tabs">
        <button [class.active]="tab() === 'classes'" (click)="tab.set('classes')">Classes</button>
        <button [class.active]="tab() === 'subjects'" (click)="tab.set('subjects')">Subjects</button>
        <button [class.active]="tab() === 'sections'" (click)="tab.set('sections')">Sections</button>
        <button [class.active]="tab() === 'timelines'" (click)="tab.set('timelines')">Timelines</button>
      </div>
      @switch (tab()) {
        @case ('classes') {
          <table><thead><tr><th>Name</th><th>Description</th><th>Status</th></tr></thead><tbody>
            @for (c of classes(); track c.id) { <tr><td>{{c.name}}</td><td>{{c.description||'-'}}</td><td><span [class]="c.isActive?'on':'off'">{{c.isActive?'Active':'Inactive'}}</span></td></tr> }
            @empty { <tr><td colspan="3" class="empty">No classes</td></tr> }
          </tbody></table>
        }
        @case ('subjects') {
          <table><thead><tr><th>Name</th><th>Description</th><th>Status</th></tr></thead><tbody>
            @for (s of subjects(); track s.id) { <tr><td>{{s.name}}</td><td>{{s.description||'-'}}</td><td><span [class]="s.isActive?'on':'off'">{{s.isActive?'Active':'Inactive'}}</span></td></tr> }
            @empty { <tr><td colspan="3" class="empty">No subjects</td></tr> }
          </tbody></table>
        }
        @case ('sections') {
          <table><thead><tr><th>Name</th><th>Capacity</th><th>Status</th></tr></thead><tbody>
            @for (s of sections(); track s.id) { <tr><td>{{s.name}}</td><td>{{s.capacity}}</td><td><span [class]="s.isActive?'on':'off'">{{s.isActive?'Active':'Inactive'}}</span></td></tr> }
            @empty { <tr><td colspan="3" class="empty">No sections</td></tr> }
          </tbody></table>
        }
        @case ('timelines') {
          <table><thead><tr><th>Name</th><th>Start</th><th>End</th><th>Status</th></tr></thead><tbody>
            @for (t of timelines(); track t.id) { <tr><td>{{t.name}}</td><td>{{t.startDate|date:'mediumDate'}}</td><td>{{t.endDate|date:'mediumDate'}}</td><td><span [class]="t.isActive?'on':'off'">{{t.isActive?'Active':'Inactive'}}</span></td></tr> }
            @empty { <tr><td colspan="4" class="empty">No timelines</td></tr> }
          </tbody></table>
        }
      }
    </div>
  `,
  styles: [`
    .page{padding:2rem;max-width:1200px;margin:0 auto}header{margin-bottom:1.5rem}.back{color:#667eea;text-decoration:none;display:inline-block;margin-bottom:.5rem}h1{margin:0}
    .tabs{display:flex;gap:.5rem;margin-bottom:1.5rem;border-bottom:2px solid #eee;padding-bottom:.5rem}
    .tabs button{padding:.75rem 1.5rem;border:none;background:none;cursor:pointer;font-size:1rem;color:#666;border-radius:4px 4px 0 0}
    .tabs button.active{background:#667eea;color:#fff}
    table{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.1);overflow:hidden}
    th,td{padding:1rem;text-align:left;border-bottom:1px solid #eee}th{background:#f8f9fa;font-weight:600}
    .empty{text-align:center;color:#999}.on{background:#d4edda;color:#155724;padding:.25rem .75rem;border-radius:12px;font-size:.85rem}
    .off{background:#f8d7da;color:#721c24;padding:.25rem .75rem;border-radius:12px;font-size:.85rem}
  `]
})
export class CurriculumComponent implements OnInit {
  tab = signal<'classes'|'subjects'|'sections'|'timelines'>('classes');
  classes = signal<ClassDto[]>([]); subjects = signal<SubjectDto[]>([]); sections = signal<SectionDto[]>([]); timelines = signal<TimelineDto[]>([]);
  constructor(private svc: CurriculumService) {}
  ngOnInit() {
    this.svc.getClasses(1,100).subscribe(r => this.classes.set(r.items));
    this.svc.getSubjects(1,100).subscribe(r => this.subjects.set(r.items));
    this.svc.getSections(1,100).subscribe(r => this.sections.set(r.items));
    this.svc.getTimelines(1,100).subscribe(r => this.timelines.set(r.items));
  }
}
